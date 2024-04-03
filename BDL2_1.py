from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import random
import os
from bs4 import BeautifulSoup
import random
import zipfile

#output_zip_path = '/home/keshava/weather_files/file.zip' 
#Specifying the location of the final zipfile
output_zip_path=input('Enter the path for the final zip file:') #The path must contain the file name and the specified directory must exist
base_url='https://www.ncei.noaa.gov/data/local-climatological-data/access/' #The base url from which we collect the data
current_directory = os.getcwd().replace(' ','\ ') #replace(' ','\ ') is used to account for file names which may contain spaces
#year = '1999' 
#Specifying the year we require the data for
year=input('Enter the year for which the data is fetched:')
#num_files = 3 
#Specifying the number of files to be fetched
num_files=int(input('Specify the number of files to be fetched:'))
'''
	This is a helper function that selects a specific number of random files from all the files available for the specified year. This task is carried out after fetching the page. 
'''
def select_random_files(year, num_files):
    
    with open(year,'r') as page: #read the fetched html page as a string
        text=page.read()
    soup = BeautifulSoup(text, 'html.parser')
    all_csv = [link.get('href') for link in soup.find_all('a', href=True) if link['href'].endswith('.csv')] #use BeautifulSoup to obtain a list of the names of all the csv files in the page
    page.close()
    if len(all_csv)>num_files: #To account for the case where the specified number of files is greater than the total number of available files
    	sampled_csv=random.sample(all_csv,num_files)
    else:
    	sampled_csv=all_csv
    with open('selected_random_files','w') as file: #Store the selected random file links in a text file so that the csv files can be fetched using wget
        for csv in sampled_csv:
            file.write(base_url+year+'/'+csv+'\n')
    file.close()
'''
	This is a helper function that zips all the files in a specified folder and stores it in a given location. This task is carried out after fetching the files.
'''
def zip_csv_files(input_dir, output_zip):
    with zipfile.ZipFile(output_zip, 'w') as zipf:
        for root, _, files in os.walk(input_dir):
            for file in files:
                zipf.write(os.path.join(root, file), arcname=file) #write each file to the zip file
                
    zipf.close()

    
'''
	Define the DAG
'''
with DAG(
    dag_id='BDL2_1',
    description='Pipeline to fetch and process NOAA data',
    schedule=None,
    
    tags=['noaa', 'data']
) as dag: 

    fetch_page_task = BashOperator(
        task_id='fetch_page',
        bash_command=f'wget {base_url}{year} -P {current_directory}', dag = dag
    ) #Bash operator to fetch the page corresponding to the specified year using wget

    select_files_task = PythonOperator(
        task_id='select_files',
        python_callable=select_random_files,
        op_kwargs={'year': year, 'num_files': num_files}
    ) #Python Operator to select specified number files randomly from the fetched page
    

    fetch_files_task = BashOperator(
        task_id='fetch_files',
        bash_command=f'wget --input-file={current_directory}/selected_random_files -P {current_directory}/Data',dag=dag
    ) #Bash operator to fetch the randomly selected csv files using wget

    zip_files_task = PythonOperator(
        task_id='zip_files',
        python_callable=zip_csv_files,  # Define this function to zip the fetched files
        op_kwargs={'input_dir': 'Data', 'output_zip': 'file.zip'},dag=dag
    ) #Python Operator to zip the fetched csv files

    move_files_task = BashOperator(
        task_id='move_files',
        bash_command=f'mv {current_directory}/file.zip {output_zip_path}'
    )#Bash operator to move the zip file to a desired location 

    fetch_page_task >> select_files_task >> fetch_files_task >> zip_files_task >> move_files_task

if __name__=='__main__':
	dag.test()
