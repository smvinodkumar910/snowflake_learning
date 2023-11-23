import requests
import pandas as pd
from datetime import datetime as dt
import pytz



city_list=['chennai','mumbai','pune','hyderabad','bengaluru','surat','kolkata','ahmedabad','visakhapatnam','madurai']


url = "https://weatherapi-com.p.rapidapi.com/current.json"



headers = {
	"X-RapidAPI-Key": "11e969206dmsh2dc567692521a17p1aff25jsnf8d1e65d23f9",
	"X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com"
}

location=[]
weather_details=[]
current_condition=[]

for city in city_list:
    querystring = {"q":city}
    response = requests.get(url, headers=headers, params=querystring)
    output = response.json()
    loc=output.get("location")
    cc = output.get("current").get("condition")
    wd = output.get("current")

    del wd['condition']
    wd['city']=loc.get("name")

    cc['weather_time'] = wd.get("last_updated")
    
    location.append(loc)
    current_condition.append(cc)
    weather_details.append(wd)


loc_df = pd.DataFrame(location)
cc_df = pd.DataFrame(current_condition)
wd_df = pd.DataFrame(weather_details)
print(loc_df)
print(cc_df)
print(wd_df)

utc_time = dt.now(pytz.timezone('utc'))

file_path = utc_time.strftime('%Y%m%d%H%M%S')


loc_df.to_csv("LOCATION_DTL_"+file_path+".csv", index=None)
cc_df.to_csv("CURRENT_CONDITION_DTL_"+file_path+".csv", index=None)
wd_df.to_csv("WEATHER_DETAILS_DTL_"+file_path+".csv", index=None)