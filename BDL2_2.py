from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.utils.dates import days_ago
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.runners.interactive.interactive_beam as ib
import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt


#zip_location='/home/keshava/weather_files/file.zip'
zip_location=input('Enter the path to the zip folder:') #Specify the path to the zip folder
cwd=os.getcwd() #Get current working directory
'''
    This is a helper function which reads each csv file and filters based on the required fields
'''
def read_data(csv_file):
    import pandas as pd
    required_fields=['DATE','HourlyDryBulbTemperature','HourlyWindSpeed'] #Dry Bulb Temperature and Windspeed are the features used
    df = pd.read_csv(csv_file) #read the csv file
    #print(df.head())
    lat=df['LATITUDE'].iloc[0] #Obtain the latitude of the place
    lon=df['LONGITUDE'].iloc[0] #Obtain the longitude of the place
    data=df[required_fields] #Filter based on the required fields
    #required_fields.insert(0,'DATE')
    
    for field in required_fields:
        if field!='DATE':
            data[field]=pd.to_numeric(data[field],errors='coerce') #Convert the data from string to float
        else:
            data[field]=data[field]
    data=data.dropna() #To drop empty rows
    data=data.values.tolist() #Convert the dataframe to a list
    #data_final=[[float(element) for element in row] for row in data]
    result = (lat,lon,data) # The result is a tuple of the form <Lat, Lon, [[ArrayOfHourlyDataOfTheReqFields]]>
    
    return result
'''
    This function implements the beam pipeline to extract the data.
'''
def extract_data():     
    cwd = os.getcwd() #get the current working directory
    beam_options = PipelineOptions(
        runner='DirectRunner',
        project='Big-Data-Lab',
        job_name='Analytics-task',
        temp_location='/tmp',
        direct_num_workers=6,
        direct_running_mode='multi_processing' 
    ) #Define the beam pipeline options as required
    
    all_files = os.listdir(cwd)
    csv_files = [os.path.join(cwd,file) for file in all_files if file.endswith('.csv')] #Create a list of the csv files that has to be extracted
    #It is assumed that there are no other csv files in the current working directory apart from the required weather data files
    #print(list_of_files)
    pipeline = beam.Pipeline(options = beam_options) #Create the pipeline
    
    #fields = pipeline | "required_Fields" >> beam.Create(['DATE','HourlyDryBulbTemperature','HourlyWindSpeed']) #Specify the required fields
    extracted_data = (  pipeline
                        | beam.Create(csv_files)
                        | beam.Map(read_data) #Extract the contents of the csv files
                        | beam.io.WriteToText(f'{cwd}/extracted_files') #Store the output
                     )
    pipeline.run() #Run the beam pipeline
    
'''
	Helper to extract the months and the required fields from the output of the extract data task
'''
class ExtractMonthAndValues(beam.DoFn):
    def process(self, element):
        latitude, longitude, data = element
        for entry in data:
            date_time,temperature, windspeed = entry
            month = date_time.split('-')[1][:2]  # Extracting the month from the date_time
            yield ((latitude, longitude, month), (float(temperature), float(windspeed)))

'''
	Helper to compute the monthly averages
'''
class CalculateMonthlyAverage(beam.DoFn):
    def process(self, element):
        key, values = element
        temp_sum = 0
        wind_sum = 0
        count = 0
        for temp, wind in values:
            temp_sum += temp
            wind_sum += wind
            count += 1
        yield (key, (temp_sum / count, wind_sum / count)) #Computing the required averages

'''
	This function implements the beam pipeline to compute the monthly averages
'''
def compute_monthly_averages():
    cwd=os.getcwd() #Obtain current working directory
    all_files=os.listdir(cwd) #All the files in the current working directory
    input_files=[file for file in all_files if ('extracted_files' in file and 'temp' not in file)] #Considering only the files obtained from the extract data task
    count=1 #Variable to keep track of the number of the input_files 
    for input_file in input_files:
        output_file = f"monthly_avg/monthly_avg-{count}" #A new folder called monthly_avg is created and the outputs are stored there 
        count+=1 #To give different names to the outputs of different files
        with beam.Pipeline(options=PipelineOptions()) as p:
            data = p | "ReadInputData" >> beam.io.ReadFromText(input_file) #Read the extracted data
        
            monthly_averages = (data
                                | "ParseData" >> beam.Map(eval)  # Assuming tuples are stored as strings in the text file
                                | "ExtractMonthAndValues" >> beam.ParDo(ExtractMonthAndValues()) #Extract the month and fields
                                | "GroupByMonth" >> beam.GroupByKey() 
                                | "CalculateMonthlyAverage" >> beam.ParDo(CalculateMonthlyAverage())) #Calculate the monthly averages
        
            
            def remove_month(element): #Helper to format the monthly averages before grouping them by (latitude,longitude)
                (latitude, longitude, month), (avg_temp, avg_wind) = element
                return (latitude,longitude),[avg_temp,avg_wind]
            def format_output(element): #Helper to format the output to the required format
            	(latitude,longitude),Array_of_monthly_averages=element
            	return f"({latitude},{longitude},{Array_of_monthly_averages})"
        
            output = (monthly_averages | "RemoveMonth" >> beam.Map(remove_month) #Remove the month since it is not required from now on
            			       | "GroupBylatitudelongitude" >> beam.GroupByKey() #Group all the values
            			       | "FormatOutput" >> beam.Map(format_output)) #Format the output to the required format
            # The result is a tuple of the form <Lat, Lon, [[ArrayOfMonthlyAveragesOfTheReqFields]]>
            output | "WriteOutput" >> beam.io.WriteToText(output_file) #Write the output to a text file
            
'''
    Helper to extract the monthly average files and return the latitude, longitude and the temperature and windspeed of the required month
'''
class ExtractTemperatureAndWindspeed(beam.DoFn): 
    def process(self, element):
        data = eval(element.strip())
        latitude, longitude = data[0], data[1]
        temperature, windspeed = data[2][0] #Choosing january month, 0=jan, 1=feb and so on...
        yield (latitude, longitude, temperature, windspeed)
'''
    Helper to plot and visualize the monthly averages for temperature and windspeed
'''
def plot_heatmap(data):
    latitudes = []
    longitudes = []
    temperatures = []
    windspeeds = []

    for item in data:
        latitudes.append(item[0])
        longitudes.append(item[1])
        temperatures.append(item[2])
        windspeeds.append(item[3])

    gdfT = gpd.GeoDataFrame({
        'Latitude': latitudes,
        'Longitude': longitudes,
        'Temperature': temperatures,
        
    }) #geopandas dataframe  for plotting temperature data

    gdfW = gpd.GeoDataFrame({
        'Latitude': latitudes,
        'Longitude': longitudes,
        'Windspeed': windspeeds
    }) #geopandas dataframe for plotting the windspeed data

    gdfT['geometry'] = gpd.points_from_xy(gdfT['Longitude'], gdfT['Latitude'])

    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))

    fig, ax = plt.subplots(figsize=(10, 10))
    world.plot(ax=ax, color='lightgrey')
    gdfT.plot(ax=ax, column='Temperature', cmap='coolwarm', marker='o', markersize=50, legend=True, #Heatmap based on temperature
             legend_kwds={'shrink': 0.5})  # Adjust the size of the legend bar here
    plt.title('Heatmap of Dry Bulb Temperature for January Month') #Change title depending on the month
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    #plt.show()
    plt.savefig('Temperature_Jan.png',dpi=200,bbox_inches='tight') #The plot is stored as a png file in the current working directory
    plt.close()
    gdfW['geometry'] = gpd.points_from_xy(gdfW['Longitude'], gdfW['Latitude'])

    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres')) #Our data is plotted on top of this world data

    fig, ax = plt.subplots(figsize=(10, 10))
    world.plot(ax=ax, color='lightgrey')
    gdfW.plot(ax=ax, column='Windspeed', cmap='coolwarm', marker='o', markersize=50, legend=True, #Heatmap based on windspeed
             legend_kwds={'shrink': 0.5})  # Adjust the size of the legend bar here
    plt.title('Heatmap of Windspeed for January Month') #Change title depending on the month
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    #plt.show()
    plt.savefig('Windspeed_Jan.png', dpi=200, bbox_inches='tight') #he plot is stored as a png file in the current working directory
    plt.close()
'''
    The function which implements the beam pipeline to create visualization for monthly averages
'''
def create_visualization():
    with beam.Pipeline() as pipeline:
        files = pipeline | "Read Files" >> beam.io.ReadFromText("monthly_avg/") #Read the data obtained from compute_monthly_averages_task
    
        data = (
            files
            | "Extract Data" >> beam.ParDo(ExtractTemperatureAndWindspeed())
        ) #Extract the data
    
        plot_data = data | "Collect Plot Data" >> beam.combiners.ToList()
        plot_data | "Plot Heatmap" >> beam.Map(plot_heatmap) #Plot the data and save it as PNG

'''
   Define the DAG 
'''
with DAG(
    dag_id='Analytics_Pipeline',
    description='Pipeline to process fetched data',
    schedule_interval='*/1 * * * *', #Auto trigger every 1 min
    start_date=datetime(2024, 3, 3)

) as dag:
    wait_for_archive = FileSensor(
        task_id='wait_for_archive',
        poke_interval=5,  # Check every 5 seconds
        timeout=5,  # Timeout after 5 seconds
        filepath=zip_location,
        dag=dag,
    ) #Wait for the archive to arrive using File Sensor
    check_zip_file = BashOperator(
    task_id='check_zip_file',
    bash_command=f'if [[ $(file {zip_location}) == *"Zip archive data"* ]]; then echo "File is a zip file"; else echo "File is not a zip file"; exit 1; fi',
    dag=dag,
    ) #Check if the file in the path is a zip file
    unzip_file = BashOperator(
    task_id='unzip_file',
    bash_command=f'unzip -o {zip_location} -d {cwd}',
    dag=dag,
    )#Bash operator to unzip the zip file using unzip command

    extract_data_task = PythonOperator(
    task_id='extract_and_process_data',
    python_callable=extract_data,
    dag=dag,
    )#Python operator to extract the csv file, filter the dataframe based on required fields and extract the latitude and longitude information 
    
    compute_average_task = PythonOperator(
    task_id='compute_the_monthly_average_for_the_task',
    python_callable=compute_monthly_averages,
    dag=dag,
    )#Python operator to compute the monthly averages of the extracted data
    
    create_visualization_task = PythonOperator(
    task_id='Create_visualization_for_monthly_average_data_and_store_it_as_png',
    python_callable=create_visualization,
    dag=dag,
    )#Python Operator to create visualization of the monthly averages computed in the previous task
    
    delete_files_task=BashOperator(
    task_id='Delete_the_unzipped_csv_files',
    bash_command=f'rm {cwd}/*.csv',
    dag=dag,
    )#Bash Operator to delete the csv files  
    
    
wait_for_archive >> check_zip_file >> unzip_file >> extract_data_task >> compute_average_task >> create_visualization_task >> delete_files_task
dag.test()
