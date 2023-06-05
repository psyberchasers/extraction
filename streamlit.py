import os
import base64
import streamlit as st
import pandas as pd
import tempfile

from langchain.document_loaders import UnstructuredFileLoader
from tools.CustomCharacterTextSplitter import CustomCharacterTextSplitter
from utils.extractData import extract_data


def get_table_download_link(df):
    """Generates a link allowing the data in a given panda DataFrame to be downloaded
    in:  DataFrame
    out: href string
    """
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(
        csv.encode()
    ).decode()  # some strings <-> bytes conversions necessary here
    href = f'<a href="data:file/csv;base64,{b64}" download="mydata.csv">Download csv file</a>'
    return href


st.title("Langchain Chat")
st.header("PDF Mode")

if "last_uploaded_files" not in st.session_state:
    st.session_state.last_uploaded_files = []

openai_api_key = st.sidebar.text_input("OpenAI API Key")

if openai_api_key:
    st.write("Using key:", openai_api_key)

uploaded_files = st.sidebar.file_uploader(
    "Upload multiple files", accept_multiple_files=True, type="pdf"
)

if uploaded_files:
    output = []
    with tempfile.TemporaryDirectory() as tmpdir:
        text_splitter = CustomCharacterTextSplitter(max_chunks=1, chunk_size=3000)
        for uploaded_file in uploaded_files:
            file_name = uploaded_file.name
            file_content = uploaded_file.read()
            file_path = os.path.join(tmpdir, file_name)
            with open(file_path, "wb") as file:
                file.write(file_content)
                loader = UnstructuredFileLoader(file_path)
                documents = loader.load()
                documents = text_splitter.split_documents(documents)
                with st.spinner("Generating response..."):
                    data = extract_data(documents, api_key=openai_api_key)
                    output.extend(data)

    # Show data as table
    df = pd.DataFrame(output)
    df.columns = df.columns.str.capitalize()
    st.write(df)
    st.markdown(get_table_download_link(df), unsafe_allow_html=True)
