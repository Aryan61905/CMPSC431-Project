import requests
from bs4 import BeautifulSoup
import pandas as pd

def scrape_tables_to_csv(url, output_folder='tables_output'):
    # Send a GET request to the URL
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content of the page
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all tables in the page
        tables = soup.find_all('table')
        
        # Create the output folder if it doesn't exist
        import os
        os.makedirs(output_folder, exist_ok=True)
        
        # Loop through each table and store it in a separate CSV file
        for i, table in enumerate(tables):
            # Convert the table to a pandas DataFrame
            df = pd.read_html(str(table))[0]
            
            # Generate the filename for the CSV file
            filename = f"{output_folder}/table_{i+1}.csv"
            
            # Write the DataFrame to a CSV file
            df.to_csv(filename, index=False)
            
            print(f"Table {i+1} saved to {filename}")
    else:
        print("Failed to retrieve data from the URL")


url = 'https://www.basketball-reference.com/leagues/NBA_2024.html'  
scrape_tables_to_csv(url)