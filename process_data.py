from urllib.request import urlopen, Request
import pandas as pd
import json
import sys

class Process():
    """
    Class for preprocessing and saving api data

    ...

    Attributes
    ----------
    URL : str
        a formatted string to represent the api url
    headers : dict
        headers for api request
    

    Methods
    -------
    load_data(filename)
        Makes the api request and saves the processed data to disk as a csv file
    """

    URL = 'https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=7e181136-d81a-48a8-9350-3f09265db3c7&limit=5'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.3'}

    def __init__(self):
        pass

    def load_data(self,filename):
        json_data = None


        new_req = Request(self.URL,headers=self.headers)

        try:
            with urlopen(new_req) as req:
        
                data = req.read()
        
                encoding = req.info().get_content_charset('json')
                json_data = json.loads(data.decode(encoding))
        
                print(json_data)
                results = json_data['result']['records']

        except Exception as e:
            print("Failed to open url or connect")

        # Loading the data into pandas dataframe
        results_df = pd.DataFrame(results)

        # setting end_of_yea as index
        results_df.set_index('end_of_year',inplace=True)

        # dropping the row with idex 2014 as it contains null values
        results_df.drop(results_df.loc[results_df.index==2014].index,inplace=True)

        try:
            results_df.to_csv(filename)

        except Exception as e:
            print("Failed to write to disk..")

        else:
            print("File successfully saved to disk with {}".format(filename))



# instantiate the Process class
process_obj = Process()

# get the file extension from command line args
file_extension = sys.argv[1].split('.')[1]

if file_extension == 'csv':
    process_obj.load_data(sys.argv[1])
    
else:
    raise ValueError("Please provide a valid filename..!")

