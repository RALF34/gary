import subprocess
import time
from datetime import date, datetime, timedelta

import requests
from matplotlib import pyplot
from pymongo import MongoClient

overseas_departments = [
    "GUADELOUPE",
    "GUYANE",
    "MARTINIQUE",
    "LA REUNION",
    "MAYOTTE",
    "SAINT-MARTIN"
]

symbol_to_name = {
    "O3": "ozone",
    "NO2": "nitrogen dioxide",
    "SO2": "sulphur dioxide",
    "PM2.5": "fine particles",
    "PM10": "particles",
    "CO": "carbone monoxide"
}

WHO_recommendation = {
    pollutant: value for (pollutant, value) in zip(
        symbol_to_name.keys(),
        [100,25,40,15,45,4]        
    )
}

mongoClient = MongoClient("mongodb://localhost:8001")
database = mongoClient["air_quality"]

def get_items(about, query_filter):
    '''
    Query the "air_quality" database to retrieve the items
    representing the available choices proposed to the user.

    Arguments:
    about -- string determining the name of the collection
             to query within the database.
    query_filter -- dictionary used as filter for the query.
    '''
    # Query the appropriate collection and store the retrieved
    # elements in a list "items".
    match about:
        case "regions":
            items = database["regions"].find().distinct("_id")
            for e in overseas_departments:
                items.remove(e)
        case "departments":
            if query_filter["_id"] == "OUTRE-MER":
                items = overseas_departments
            else:
                items = list(set(database["regions"].find_one(
                    query_filter)["departments"]))
        case "cities":
            items = list(set(database["departments"].find_one(
                query_filter)["cities"]))
        case "stations":
            list_of_stations = database["cities"].find_one(
                query_filter)["stations"]
            items = list(set([
                e["name"]+"#"+e["code"]
                for e in list_of_stations]))
        case "pollutants":
            items = list(set(database["distribution_pollutants"].find_one(
                query_filter)["monitored_pollutants"]))
    # Build the "listed_items" list giving the ordered set of the retrieved
    # items along with their corresponding position.
    listed_items = list(zip(sorted(items), range(1,len(items)+1)))
    if about == "regions":
        listed_items.append((len(listed_items)+1,"OUTRE-MER"))
    return listed_items


all_the_stations = database["distribution_pollutants"].distinct("_id")

def is_number(string):
    '''
    Return True if "string" represents a positive integer,
    False otherwise.
    '''
    try:
        int(string)
        return True
    except ValueError:
        return False

def get_input(about, items, shorter_period=False, first_choice=False):
    '''
    Display the available choices, check validity of the input and return it.

    Arguments:
    about -- string determining the message displayed to the user.
    items -- list containing the choices proposed to the user.
    shorter_period -- boolean set to True when treating the specific case of
                      allowing the user to reduce the number of pollution days
                      taken into account.
    '''
    if about not in ["pollutants","n_days"]:
        message_to_user = "Select a French "+about+"."
        choices = [
            str(e[1])+" : "+(e[0] if not(about=="stations") 
            else e[0][:e[0].index("#")]) for e in items]
    elif about == "pollutants":
        message_to_user = "Select an air pollutant."
        choices = [str(e[1])+" : "+e[0] for e in items]
    elif not(shorter_period):
        message_to_user = "The pollution analysis is based on data collected\n\
        over the last 180 days. Do you want to consider a shorter period? (Y/n)"
        choices = []
    else:
        message_to_user = "Enter a number of days.\n"
        choices = []
    # Add the "Return" option allowing to return to the previous
    # choices.
    if about != "regions":
        choices += [
            "\n"+str(len(lines)+1)+ " : Return"]
    # Ask the user for his choice and save the "answer".
    text = message_to_user+"\n".join(choices)+"\n\n"
    space = "\n" if (about == "regions" and first_choice) else "\n"*4
    answer = input(space+text)
    # If a number is expected as input...
    if not(about=="n_days" and not(shorter_period)):
        # Assign to "n" the highest possible value for the input.
        n = 180 if about == "n_days" and shorter_period else len(choices)+1
        # Ask the same question to the user until a valid answer is provided.
        while not(is_number(answer) and int(answer) in range(1,n+1)):
            answer = input("\n"*4+text)
        number = int(answer)
        # Check avaibility of pollution data recorded by the chosen station.
        if number < n and about == "stations":
            item = choices[number-1][1]
            station_found = item[item.index("#")+1:] in all_the_stations
            if not(station_found):
                print("Sorry, no data available for this station.\n")
                return None

        return number
    else:
        while answer not in ["Y","y","n"]:
            answer = input("\n"*4+text)
        if answer in ["Y","y"]:
            return "180"
        else:
            return get_input(about, items, shorter_period=True)

steps = ["regions","departments","cities","stations","pollutants","n_days"]

class userChoices():
    '''
    Interact with the user to retrieve the query string parameters to send
    to the API endpoint.
    '''
    def __init__(self):
        self.query_parameters = {
            "s": None,
            "station_name": None,
            "p": None,
            "n": None}
        self.i = 0 # saves the current step.
        self.current_filter = None
        self.previous_filter = None
        self.return_back = False
        self.station_not_found = False
        self.done = False

    def get_chosen_item(self):
        '''
        Display the available choices and save the provided input.
        '''
        current_step = steps[self.i]
        # Retrieve items from the database.
        if not(self.i):
            items = get_items(current_step, {})
        elif current_step != "n_days":
            items = get_items(
                current_step,
                search_filter=self.current_filter)
        # No items listed at the last step.
        else:
            items = []
        
        x = get_input(current_step, items)
        # Check whether the present case is not the one where the station
        # chosen by the user does not provide any pollution data.
        if type(x) is int:
            if self.i != 5:
                choicen_item = None if x == len(listed_items)+1 else \
                listed_items[x-1][1]
            else:
                chosen_item = x
            if chosen_item is None:
                self.return_back = True
            else:
                self.previous_filter = self.current_filter
                if current_step in ["regions","departments","cities"]:
                    self.current_filter = {"_id": chosen_item}
                elif current_step == "stations":
                    name, code = chosen_item.split(sep="#")
                    self.query_parameters["s"] = code
                    self.query_parameters["station_name"] = name
                    self.current_filter = {"_id": code}
                elif current_step == "pollutants":
                    self.query_parameters["p"] = chosen_item
                else:
                    self.query_parameters["n"] = chosen_item
                    # Indicate that the saving of the query parameters is done.
                    self.done = True
        else:
            self.station_not_found = True
    def next_step(self):
        '''
        Move the process one step forward or one step backward
        depending on the user's choice.
        '''
        if self.return_back:
            self.i -= 1
            self.current_filter = self.previous_filter
            self.return_back = False
        else:
            if self.station_not_found:
                self.done = True
            self.i += 1
    

def plot_variation(station, pollutant, values):
    '''
    Generate the graph showing average daily variation (obtained using
    average concentrations recorded at each of the 24 hours of the day, 
    stored in "values") of air concentration of "pollutant" recorded by 
    "station".
    '''
    fig, ax = pyplot.subplots()
    fig.set_size_inches(17,14)
    ax.scatter([str(x)+"h00" for x in range(24)], values)
    # Compute four threshold values based on the corresponding WHO
    # recommendation (will be used later to split the graph into
    # colored zones, improving readibility and understanding of the
    # displayed pollution data).
    thresholds = [
        (x/3)*WHO_recommendation[pollutant]
        for x in range(1,5)]
    ax.plot(
        range(24),
        [thresholds[2]]*24,
        color="blueviolet",
        ls="--",
        lw=1.7,
        label="Average daily air\nconcentration\nrecommended by WHO")
    # Determine the maximum value to consider for the Y-axis in order
    # to avoid scaling issues which could affect readibility of the
    # displayed data.
    highest_value = max(values)
    max_level = 2
    while (max_level < 5 and thresholds[max_level] < highest_value):
        max_level += 1
    if max_level == 5:
        max_level -= 1
    space = (0.40)*thresholds[0]
    lim = thresholds[max_level] if max_level == 2 else highest_value
    ax.set_ylim(0,lim+(space if max_level == 2 else 0))
    # Split the graph into four colored zones.
    colors = ["limegreen","yellow","orange","red"]
    y_min = 0
    for j in list(range(max_level+1)):
        ax.fill_between(
            list(range(24)),
            thresholds[j],
            y2=y_min,
            color=colors[j],
            alpha=0.1)
        y_min = thresholds[j]
    # Add a fifth zone if one or several values are above the highest
    # set threshold.
    if highest_value > thresholds[max_level]:
        ax.fill_between(
            list(range(24)),
            ax.get_ylim()[1],
            y2=thresholds[max_level],
            color="magenta",
            alpha=0.1)
    ax.set_yticks([0])
    ax.set_yticklabels([" "])
    ax.legend(loc="upper right")
    ax.set_title(
        "Average daily"+symbol_to_name[pollutant]+" pollution\n\
        recorded at :\n"+station,
        ha="center")
    pyplot.savefig("image")

def main():
    
    # Display a message to the user if the initialization process
    # of the pollution data is still running.
    i = 0
    while "last_update" not in database.list_collection_names():
        if i == 4:
            i = 0
        print(
            " Sorry, the initialization of the database\nis not complete"+
            ("    " if not(i) else "."*i),
            end="\r")
        i += 1
        time.sleep(0.7)

    DATE = date.today() - timedelta(days=1)
    DATETIME = datetime(DATE.year, DATE.month, DATE.day)
    # If some pollution days are missing from the database, send a special
    # request (with a pollution period of "zero day") to allow the server to
    # perform an update of the data.
    if database["last_update"].find_one()["date"] != DATETIME:
        dictionary = database["distribution_pollutants"].find_one()
        parameters = {
            "s": dictionary["_id"],
            "p": dictionary["monitored_pollutants"][0],
            "n": "0"}
        _ = requests.get(
            "http://127.0.0.1:8000",
            params=parameters,
            verify=False)
    # Start the process of interacting with the user to get the query parameters
    # corresponding to his choices.
    process = userChoices()
    while not(process.done):
        process.get_chosen_item()
        process.next_step()
    # Send the given query parameters to the endpoint and retrieve the values
    # provided by the response.
    values = requests.get(
        "http://127.0.0.1:8000",
        params=process.query_parameters,
        verify=False).json()["values"]
    # Generate the expected graph and display it to the user.
    plot_variation(
        process.parameters["station_name"],
        process.parameters["p"],
        values)
    subprocess.run(["xdg-open","image.png"])

if __name__=="__main__":
    main()
