import os
import sys

import constants

os.environ["OPENAI_API_KEY"] = constants.openai_api_key

from langchain_openai import ChatOpenAI

model = ChatOpenAI()

query = sys.argv[1]



from LangChain.document_loaders import CSVLoader
from langchain.indexes import VectorstoreIndexCreator
from langchain.indexes.vectorstore import VectorStoreIndexWrapper

loader = CSVLoader(file_path='D:\\Work\\AI\\LangChain\\enriched_oi_data.csv',
    csv_args={
    'delimiter': ',',
    'quotechar': '"',
    'fieldnames': ['symbol','openInterest','buildUp','ltp','ltpChange','ltpChangePercent','openInterestChange','token','Stock Symbol','Stock Token','Timestamp']
})

 index = VectorstoreIndexCreator().from_loaders([loader])

 print(index.query(query))