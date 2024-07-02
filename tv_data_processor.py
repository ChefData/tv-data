import pandas as pd

class TVDataProcessor:
    def __init__(self, file_path: str):
        """
        Initialise the TVDataProcessor with the path to the Excel file.

        Args:
            file_path (str): Path to the Excel file.
        """
        self.file_path = file_path
        self.df_viewings: pd.DataFrame | None = None
        self.df_producers: pd.DataFrame | None = None
        self.final_summary: pd.DataFrame | None = None

    def load_data(self) -> None:
        """Load data from the Excel file into DataFrames."""
        try:
            self.df_viewings = pd.read_excel(self.file_path, sheet_name='Viewing data')
            self.df_producers = pd.read_excel(self.file_path, sheet_name='Producers')
        except FileNotFoundError as e:
            print(f"Error: The file {self.file_path} was not found.")
            raise e
        except Exception as e:
            print(f"Error: Failed to load data from {self.file_path}.")
            raise e

    def preprocess_data(self) -> None:
        """Preprocess the viewing data DataFrame."""
        try:
            self.df_viewings['Date'] = self.df_viewings['Date'].apply(pd.to_datetime, errors='coerce')
            self.df_viewings['Month'] = self.df_viewings['Date'].dt.to_period('M')
            self.df_viewings['Programme'] = self.df_viewings['Programme'].str.strip()
            self.df_viewings['Territory'] = self.df_viewings['Territory'].str.strip()
            self.df_viewings.fillna({'Viewers': 0}, inplace=True)
        except KeyError as e:
            print(f"Error: Missing expected columns in viewing data.")
            raise e

    def merge_data(self) -> None:
        """Merge the viewing data and producers DataFrames."""
        try:
            self.df_viewings = self.df_producers.merge(self.df_viewings, on='Programme', how='outer')
            self.df_viewings.fillna({'Producer': 'Unknown'}, inplace=True)
        except KeyError as e:
            print(f"Error: Missing expected columns in dataframes.")
            raise e

    def aggregate_data(self) -> None:
        """Aggregate the data to produce the summary DataFrame."""
        try:
            summary = self.df_viewings.groupby(['Producer', 'Month', 'Channel']).agg(
                Top_Programme=('Programme', lambda x: x.loc[x.idxmax()]),
                Highest_Viewers=('Viewers', 'max'),
                Sum_of_Viewers=('Viewers', 'sum')
            ).reset_index()

            self.df_viewings.sort_values(['Producer', 'Month', 'Channel', 'Date'], inplace=True)
            self.df_viewings['Cumulative_Viewers'] = self.df_viewings.groupby(['Producer'])['Viewers'].cumsum()
            cumulative_summary = self.df_viewings.groupby(['Producer', 'Month', 'Channel'])['Cumulative_Viewers'].max().reset_index()

            self.final_summary = summary.merge(cumulative_summary, on=['Producer', 'Month', 'Channel'])
        except KeyError as e:
            print(f"Error: Missing expected columns during aggregation.")
            raise e

    def convert_data_types(self) -> None:
        """Convert the data types of the final summary DataFrame."""
        try:
            self.final_summary = self.final_summary.astype({
                'Producer': 'string',
                'Month': 'string',
                'Channel': 'int',
                'Top_Programme': 'string',
                'Highest_Viewers': 'int',
                'Sum_of_Viewers': 'int',
                'Cumulative_Viewers': 'int'
            })
        except KeyError as e:
            print(f"Error: Missing expected columns during data type conversion.")
            raise e

    def save_summary_to_json(self, output_file: str) -> None:
        """Save the final summary DataFrame to a JSON file.

        Args:
            output_file (str): Path to the output JSON file.
        """
        try:
            final_summary_json = self.final_summary.to_json(orient='records', date_format='iso')
            with open(output_file, 'w') as f:
                f.write(final_summary_json)
        except Exception as e:
            print(f"Error: Failed to save data to {output_file}.")
            raise e

    def process_and_save_data(self, output_file: str) -> None:
        """Process the data and save the summary to a JSON file.

        Args:
            output_file (str): Path to the output JSON file.
        """
        try:
            self.load_data()
            self.preprocess_data()
            self.merge_data()
            self.aggregate_data()
            self.convert_data_types()
            self.save_summary_to_json(output_file)
        except Exception as e:
            print("Error: Data processing failed.")
            raise e

if __name__ == "__main__":
    file_path = '2024 Mediacells Python Task TV Data.xlsx'
    output_file = 'summary_data.json'
    processor = TVDataProcessor(file_path)
    processor.process_and_save_data(output_file)
    print(processor.final_summary)