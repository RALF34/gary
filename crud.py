import time
from datetime import date, datetime, timedelta
from statistics import mean

from bson.code import Code
from pandas import DataFrame, read_csv, read_excel
from pymongo import MongoClient

from .constants import FRENCH_DEPARTMENTS

mongoClient = MongoClient() #"mongodb://db:27017"
database = mongoClient["air_quality"]

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
    url = "https://www.lcsqa.org/system/files/media/documents/"+\
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
    # Define a function "get_department" to retrieve names of French
    # departments using postal codes of french cities.
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


def store_pollution_data(n_days, update=False):
    '''
    Create two collections storing hourly average concentrations of 
    air pollutants recorded on working days and weekends.

    Arguments:
    n_days -- number of last pollution days whose data are collected.
    update -- boolean used to determine the names of the collections
              storing the pollution data.
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
            data["dateTime"] = data["Date de début"].apply(
                lambda x: stringToDatetime(x))
            data["hour"] = data["Date de début"].apply(
                lambda x: x.hour)
            data["working_days"] = data["dateTime"].apply(
                lambda x: x.weekday())
            data = data[
                ["code site",
                "Polluant",
                "hour"
                "valeur brute",
                "dateTime",
                "working_days"]]
            # Separate the data recorded on working days from
            # those recorded on weekends.
            working_days = data[data["working_days"]]
            weekends = data[data["working_days"]==False]
            names = ("working_days","weekends") if not(update) \
            else ("new_working_days","new_weekends")
            # Update the appropriate collection.
            database[names[0]].insert_many(
                working_days.to_dict("records"))
            database[names[1]].insert_many(
                weekends.to_dict("records"))
        # Move on to the following day.
        DATE += timedelta(days=1)

def create_database():
    '''
    Create the "air quality" MongoDB database comprised of
    the following collections:
        - "cities", grouping air quality monitoring stations by cities.
        - "departments", grouping cities by French department.
        - "regions", grouping French departments by French region.
        - "LCSQA_data", containing air pollution data collected over 
           the last 180 days.
    '''
    if "air_quality" in mongoClient.list_database_names():
        mongoClient.drop_database("air_quality")
    database = mongoClient["air_quality"]
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
    
    # Create the "working_days" and "weekends" collections.
    store_pollution_data(7)
    # Create the "distribution_pollutants" collection giving, for
    # each station, the pollutant(s) whose air concentration is 
    # being recorded.
    database["working_days"].aggregate([
        {"$group":
            {"_id": "$code site",
             "monitored_pollutants":
                {"$push": "$Polluant"}}},
        {"$out": "distribution_pollutants"}])
    # Group the pollution data to allow fast calculation of the 
    # wanted averages (see function "get_values") and fast updates 
    # of the database (see function "update_database").
    for name in ["working_days","weekends"]:
        database[name].aggregate([
            {"$group":
                {"_id": {"station": "$code site",
                         "pollutant": "$Polluant",
                         "hour": "$hour"},
                 "values": {"$push": "$valeur brute"},
                 "dates": {"$push": "$dateTime"}}},
            {"$project":
                {"history": {"values": "$values",
                             "dates": "$dates"}}},
            {"$out": name}])
    # Save the current date in a new collection "last_update" 
    # (necessary to know how many pollution days are missing
    # when performing the next update).
    DATE = date.today()
    database["last_update"].insert_one(
        {"date": datetime(
            DATE.year, DATE.month, DATE.day)-timedelta(days=1)})

def update_database():
    '''
    Complete the database with the latest pollution data recorded since
    the last update and remove those being more than 180 days old.
    '''
    # Retrieve the date when the last update occured.
    last_update = database["last_update"].find_one()["date"]
    # Found the number of pollution days (given by "n_days")
    # whose data we want to add to the database.
    following_date = last_update.date() + timedelta(days=1)
    oldest_date = date.today() - timedelta(days=180)
    DATE = following_date if oldest_date < following_date \
    else oldest_date
    n_days = (date.today()-DATE).days
    # Create the "new_working_days" and "new_weekends" collections
    # storing the missing data.
    store_pollution_data(n_days, update=True)
    # Rearrange the data the same way as in the "working_days" and
    # "weekends" collections.
    for name in ["new_working_days","new_weekends"]:
        database[name].aggregate([
            {"$match": {"validité": 1}},
            {"$project": {"_id": {"station": "$code site",
                                  "pollutant": "$Polluant",
                                  "hour": "$hour"},
                          "history": {"date": "$dateTime",
                                      "value": "$valeur brute"}}}])
        # Join the collection containing the current data with the one 
        # containing the new data (line 239), update the history (line 245)
        # and update the boolean flags identifying the last values of the
        # new history (line 253).
        database[name[4:]].aggregate([
            {"$lookup": 
                {"from": name,
                 "localField": "_id",
                 "foreignField": "_id",
                 "as": name}},
            {"$set":
                {"history":
                    {"$function": 
                        {"body": updateList,
                         "args": ["$history",
                                  "$data_180_days_ago",
                                  "$"+name],
                         "lang": "js"}}}},
            {"$set":
                {"data_180_days_ago":
                    {"$eq": ["history.dates.$",
                            date.today()-timedelta(days=180)]}}},
            {"$out": name[4:]}])
        # Remove the collection used to store the new data.
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

def is_monitored_by(pollutant, station):
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
    of the 24 hours of both working days and week-end days.
    '''
    DATE = date.today()
    DATETIME = datetime(DATE.year, DATE.month, DATE.day)
    working_days, weekends = [{str(x): float(0) for x in range(24)}]*2
    query_filter = {"_id.station": station, "_id.pollutant": pollutant}
    # Check whether "n_days" is not null (the zero value is used when
    # we send the web request only to allow an update of the database).
    if n_days:
        # Retrieve all the documents with the wanted informations.
        working_days = database["working_days"].find(query_filter)
        weekends = database["weekends"].find(query_filter)
    for name in ["working_days","weekends"]:
        data = database[name].find(query_filter)
        # Check whether some data have been found.
        if data:
            # For each hour of the day retrieved from a document,
            # calculate the expected averages and update the
            # "working_days and week_end" dictionaries.
            for document in data:
                hour = document["_id"]["hour"]
                # Build list "history" with the (concentration,date) 
                # pair of values, ordered from the most recent date
                # to the oldest.
                history = list(zip(
                    document["history"]["values"],
                    document["history"]["dates"]))[::-1]
                # Extract only elements of "history" with a date less 
                # than "n_days" days before the current date (the last 
                # element to consider is given by the "i" variable at 
                # the end of the "while" loop) and update the appropriate 
                # dictionary with the wanted average.
                i = 0
                duration = DATETIME - history[i][1]
                while duration.days <= n_days:
                    i += 1
                    duration = DATETIME - history[i][1]
                averages[str(hour)] = float(mean([e[0] for e in history[:i]]))
            
    return list(working_days.values()), list(weekends.values())
