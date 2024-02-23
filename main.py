from typing import Annotated

from fastapi import FastAPI, HTTPException, Query
from pandas import read_excel
from pydantic import BaseModel, Field

from .crud import \
get_values, history_is_updated, is_monitored_by, create_database, update_database

app = FastAPI()

create_database()

# Define the "averageConcentrations" response Pydantic model
# (the interest here is on providing a description of what is
# returned by the API which will appear in the automatic 
# documentation).
class averageConcentrations(BaseModel):
    values: list[float] = Field(
        description="The average values of air concentration of the given pollutant \
        calculated for each of the 24 hours of the day (set to 0 when not enough data)\
        with data recorded by the given station over the given period."
    )

# Retrieve all the "LCSQA" station codes (will be used at line 60
# to verify the existence of the given station).
url = "https://www.lcsqa.org/system/files/media/documents/"+\
"Liste points de mesures 2021 pour site LCSQA_27072022.xlsx"
LCSQA_stations = read_excel(
    url.replace(" ", "%20"),
    sheet_name=1
).iloc[:,0][2:].tolist()

# Define the only endpoint of the API, that is a "GET" method
# returning the expected 24 average values of air concentration.
@app.get("/", response_model=averageConcentrations)
async def get_response(
    station: Annotated[
        str,
        Query(
            alias="s",
            description=(
                "Code identifying the air quality monitoring station\
                 whose data we are interested in."),
            pattern="^FR([0-9]{5}$)")],
    pollutant: Annotated[
        str,
        Query(
            alias="p",
            description=(
                "Pollutant whose average daily variation of air\
                 concentration we want to display."))],
    n_days: Annotated[
        str,
        Query(
            alias="n",
            description=(
                "Parameter telling the API that we are interested in\
                 pollution data recorded over the 'n_days' last days.")
            pattern="\d+")]):
    # Notify an error when the given station does not exist.
    if station not in LCSQA_stations:
        raise HTTPException(
            status_code=400,
            detail="This station does not exist!")
    # Notify an error when air concentration of the given 
    # pollutant is not monitored by the given station.
    if not(is_monitored_by(pollutant, station)):
        raise HTTPException(
            status_code=400,
            detail="Pollutant not available!")
    # Notify an error when the given number of days is greater than 180.
    if int(n_days) not in list(range(181)):
        raise HTTPException(status_code=400, detail="Number of days too high!")
    # Update the database if necessary.
    if not(history_is_updated):
        update_database()
    return {"values": get_values(station, pollutant, int(n_days))}
