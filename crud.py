import time
from datetime import date, datetime, timedelta
from statistics import mean

from bson.code import Code
from pandas import read_csv, read_excel
from pymongo import MongoClient

from .constants import FRENCH_DEPARTMENTS

mongoClient = MongoClient("mongodb://db:27017")
database = mongoClinet["air_quality"]

stringToDatetime = lambda x: datetime.strptime(x,"%Y/%m/%d %H:%M:%S")

updateList = Code('''
function (history, data_180_days_ago, data_yesterday){
  if (!data_180_days_ago && !data_yesterday){
    return history;
  } else if (data_180_days_ago && !data_yesterday){
    return history.slice(1);
  } else {
    const new_date = data_yesterday.date
    const new_value = data_yesterday.value
    history.dates.push(new_date);
    history.values.push(new_value);
    if (data_180_days_ago){
      history = history.slice(1)
    }
    return history;
  }
}
''')

def store_locations():
    '''
    Create mongoDB collection "LCSQA_stations storing informations
    regarding location in France of all the stations owned by the
    Central Laboratory of Air Quality Monitoring (LCSQA).
    '''
    database = mongoClient["air_quality"]
    url = "https://www.lcsqa.org/system/files/media/documents/"+
    "Liste points de mesures 2021 pour site LCSQA_27072022.xlsx"
    # Import file from previous url giving location of LCSQA stations.
    data = read_excel(url.replace(" ", "%20"), sheet_name=1)
    # Rearrange and clean the data.
    c = data.columns.tolist()
    columns_to_remove = c[3:7]+c[10:]
    labels = data.iloc[1].tolist()
    labels_to_keep = labels[:3]+labels[7:10]
    data = data.drop(
        columns=columns_to_remove
    ).drop(
        [0,1]
    ).set_axis(
        labels_to_keep,
        axis="columns")
    # Define a function "get_department" to retrieve names of French departments
    # using postal codes of french cities.
    get_department = lambda x: (
        FRENCH_DEPARTMENTS[x[:2]] if not(x[1].isdigit()) or int(x[:2]) < 97
        else FRENCH_DEPARTMENTS[x[:3]]
    )
    # Add a new column "Département" using "get_department".
    data["Département"] = data["Code commune"].apply(
        get_department)
    # Keep only columns with useful informations.
    data = data[
        ["Région",
        "Département",
        "Commune",
        "Nom station",
        "Code station"]]
    # Turn the "data" dataframe into the "LCSQA_stations" collection.
    database["LCSQA_stations"].insert_many(data.to_dict("records"))


def store_pollution_data(n_days, name):
    '''
    Create a mongoDB collection storing hourly average concentrations 
    of air pollutants recorded by LCSQA stations.

    Arguments:
    n_days -- number of last pollution days whose data are collected.
    name -- name of the collection storing the collected data.
    '''

    DATE = date.today() - timedelta(days=n_days)
    # Iterate over each day until the current day.
    while DATE < date.today():
        url = "https://files.data.gouv.fr/lcsqa/concentrations-de"+\
        "-polluants-atmospheriques-reglementes/temps-reel/"+\
        str(DATE.year)+"/FR_E2_"+DATE.isoformat()+".csv"
        data = read_csv(url, sep=";")
        # Test whether "csv" file provide some pollution data
        # (Server errors may occur, making data unavailable).
        if "validité" in data.columns:
            # Extract rows with validated data.
            data = data[data["validité"]==1]
            # Extract rows with consistent concentration value
            # (bugs during the recording process may generate 
            # negative values.)
            data = data[data["valeur brute"]>0]
            # Extract rows with pollutants of interest.
            data["pollutant_to_ignore"] = data["Polluant"].apply(
                lambda x: x in ["NO","NOX as NO2","C6H6"])
            data = data[data["pollutant_to_ignore"]==False]
            # Turn pandas dataframe into records.
            records = data.to_dict("records")
            # Iterate over each row to add new fields "dateTime" and "hour".
            for r in records:
                r["dateTime"] = stringToDatetime(r["Date de début"])
                r["hour"] = r["dateTime"].hour
                r.pop("Date de début")
            # Move data into the"LCSQA_data" collection.
            database[name].insert_many(records)
        # Move on to the following day.
        DATE += timedelta(days=1)

def create_database():
    '''
    Create the "air quality" MongoDB database comprised of
    the following collections:
        - "cities", grouping air quality monitoring stations by cities.
        - "departments", grouping cities by French department.
        - "regions", grouping French departments by French region.
        - "LCSQA_data", containing air pollution data collected over the last 180 days.
    '''

    # Create the "LCSQA_stations" collection.
    store_locations()
    # Create the "cities" collection using "LCSQA_stations".
    database["LCSQA_stations"].aggregate([
        {"$set":
            {"station": {"name": "$Nom station",
                         "code": "$Code station"}}},
        {"$group":
            {"_id": "$Commune",
             "stations": {"$push": "$station"}}},
        {"$out": "cities"}])
    # Create the "departments" collection using "LCSQA_stations"
    database["LCSQA_stations"].aggregate([
        {"$group":
            {"_id": "$Département",
             "cities": {"$push": "$Commune"}}},
        {"$out": "departments"}])
    # Create the "regions" collection using "LCSQA_stations".
    database["LCSQA_stations"].aggregate([
        {"$group":
            {"_id": "$Région",
             "departments": {"$push": "$Département"}}},
        {"$out": "regions"}])
    # Remove the "LCSQA_stations" intermediate collection.
    database.drop_collection("LCSQA_stations")
    
    # Create the "LCSQA_data" collection.
    store_pollution_data(180,"LCSQA_data")
    # Create the "distribution_pollutants" collection using "LCSQA_data".
    database["LCSQA_data"].aggregate([
        {"$group":
            {"_id": "$code site",
             "monitored_pollutants":
                {"$push": "$Polluant"}}},
        {"$out": "distribution_pollutants"}])
    # Group data in "LCSQA_data" to allow computation of wanted
    # averages (see function "get_values") and fast updates of
    # the database (see function "update_database").
    database["LCSQA_data"].aggregate([
        {"$group":
            {"_id": {"station": "$code site",
                     "pollutant": "$Polluant",
                     "hour": "$hour"},
             "values": {"$push": "$valeur brute"},
             "dates": {"$push": "$dateTime"}}},
        {"$project":
            {"history": {"values": "$values",
                         "dates": "$dates"}}},
        {"$out": "LCSQA_data"}])
    # Since the present application will be deployed using Docker containers, we     
    # can't update the history of data in a continuous way. We have to keep track of
    # the date when the last update occured, in order to know how many days we will
    # have to add to the history when performing the next update.
    # So I store this information (given by the "DATE" variable) in a new collection.
    database["last_update"].insert_one(
        {"date": datetime(DATE.year, DATE.month, DATE.day)-timedelta(days=1)})

def update_database():
    '''
    Complete the database with the latest pollution data recorded since
    the last update and remove those being more than 180 days old.
    '''
    # Retrieve the date when the last update occured.
    last_update = database["last_update"].find_one()["date"]
    # Found the number of pollution days (given by "n_days") whose data we want to 
    # add to the database.
    following_date = last_update.date() + timedelta(days=1)
    oldest_date = date.today() - timedelta(days=180)
    DATE = following_date if oldest_date < following_date \
    else oldest_date
    n_days = (date.today()-DATE).days
    # Fill the "new_data" collection with the missing data.
    store_pollution_data(n_days, "new_data")
    # Rearrange the data the same way as in the "LCSQA_data" collection.
    database["new_data"].aggregate([
        {"$match": {"validité": 1}},
        {"$project": {"_id": {"station": "$code site",
                              "pollutant": "$Polluant",
                              "hour": "$hour"},
                      "history": {"date": "$dateTime",
                                  "value": "$valeur brute"}}}])
    # Join the "LCSQA_data" collection with the "new_data" collection (line 217),
    # then update the history of pollution data (line 222) and then update the
    # boolean flags identifying the last values of the new history (line 230).
    database["LCSQA_data"].aggregate([
        {"$lookup": 
            {"from": "new_data",
             "localField": "_id",
             "foreignField": "_id",
             "as": "new_data"}},
        {"$set":
            {"history":
                {"$function": 
                    {"body": updateList,
                     "args": ["$history",
                              "$data_180_days_ago",
                              "$new_data"],
                     "lang": "js"}}}},
        {"$set":
            {"data_180_days_ago":
                {"$eq": ["history.dates.$",
                        date.today()-timedelta(days=180)]}}},
        {"$out": "LCSQA_data"}])
    # Remove the "new_data" collection from the database.
    database.drop_collection("new_data")
    # Change the date of the last update.
    database["last_update"].replace_one(
        {"date": last_update},
        {"date": datetime(
            DATE.year,
            DATE.month,
            DATE.day)-timedelta(days=1)})

def history_is_updated():
    '''
    Test whether the pollution data recorded over the last
    180 days are stored in the "air_quality" database.
    '''
    DATE = date.today()
    # Check the date of the last update to know whether some 
    # pollution days are missing.
    return database["last_update"].find_one()["date"] == \
    datetime(DATE.year, DATE.month, DATE.day) - timedelta(days=1)

def is_monitored_by(pollutant, station_code):
    '''
    Test whether air concentration of "pollutant" is recorded by the
    air quality monitoring station identified by "station_code".
    '''
    return pollutant in \
    list(set(database["distribution_pollutants"].find_one(
        {"_id": station})
    ["monitored_pollutants"]))

def get_values(station, pollutant, n_days):
    '''
    Query the "LCSQA_data" collection to retrieve average values of 
    air concentration (calculated over the "n_days" last days with
    data coming from "station") of "pollutant" associated to each 
    of the 24 hours of the day.
    '''
    DATE = date.today()
    DATETIME = datetime(DATE.year, DATE.month, DATE.day)
    # Initialize the "averages" dictionary storing the average values
    # of air concentration associated to the 24 hours of the day.
    averages = {str(x): float(0) for x in range(24)}
    # Check whether "n_days" is not null (the zero value is used when
    # we just send the web request to allow an update of the database).
    if n_days:
        # Retrieve all the documents with the wanted informations.
        data = database["LCSQA_data"].find(
            {"_id.station": station,
             "_id.pollutant": pollutant})
    # Check whether some documents have been found.
    if data:
        # Iterate over all the documents.
        for document in data:
            # Retrieve the hour when the data given by the current document 
            # have been recorded.
            hour = document["_id"]["hour"]
            # Build the "history" list composed of the 
            #(recorded_concentration-recording date) pair of values, ordered
            # from the most recent date to the oldest.
            history = list(zip(
                document["history"]["values"],
                document["history"]["dates"]))[::-1]
            # Extract only elements of "history" with a date less than "n_days"
            # days before the current date (the last element to consider is given
            # by the "i" variable at the end of the "while" loop).
            i = 0
            duration = DATETIME - history[i][1]
            while duration.days <= n_days:
                i += 1
                duration = DATETIME - history[i][1]
            # Compute the mean of the retrieved values and update the "averages"
            # dictionary accordingly.
            averages[str(hour)] = float(mean([e[0] for e in history[:i]]))
    return list(averages.values())
