import streamlit as st
import pandas as pd
import json
from tqdm import tqdm  # Import tqdm for the progress bar

def process_data_chunk(chunk):
    try:
        # Get the total number of rows in the chunk
        total_rows = len(chunk)

        # Initialize a tqdm progress bar for data cleaning
        progress_bar = tqdm(total=total_rows, desc="Cleaning Data", unit="row")

        # Perform data processing on the chunk
        chunk['attribute'] = chunk['attribute'].apply(json.loads)
        chunk_final = pd.concat([chunk.explode('attribute').drop(['attribute'], axis=1),
                                 chunk.explode('attribute')['attribute'].apply(pd.Series)], axis=1)
        # Update the progress bar
        progress_bar.update(total_rows)

        return chunk_final

    except Exception as e:
        st.error(f"Error: {e}")
        return None
    finally:
        # Close the progress bar
        progress_bar.close()

def main():
    st.title('Data Flattening App')
    st.header('Upload your csv file 😎')
    uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])

    if uploaded_file is not None:
        # Process data in chunks and display the processed data
        chunk_size = 1000  # Adjust the chunk size based on your memory constraints
        reader = pd.read_csv(uploaded_file, chunksize=chunk_size)
        df_final_chunks = []

        with st.spinner('Processing data...'):
            for chunk in tqdm(reader, desc="Processing chunks", unit="chunk"):
                df_final_chunk = process_data_chunk(chunk)
                if df_final_chunk is not None:
                    df_final_chunks.append(df_final_chunk)

        if df_final_chunks:
            # Concatenate the processed chunks and display the result
            df_final = pd.concat(df_final_chunks)
            af_final = df_final.pivot_table(index='ProductId', columns='Name', values='Value', aggfunc=lambda x: x).reset_index()
            st.subheader("Display the Cleaned Data")
            st.dataframe(df_final.head())

            st.download_button(
                label="Download your final Flattened Data",
                data=df_final.to_csv(index=False),
                file_name="Flatten Data.csv",
                mime="text/csv")

if __name__ == "__main__":
    main()
