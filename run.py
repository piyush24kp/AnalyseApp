import sys
import os
# import pandas as pd
from langchain_openai import ChatOpenAI
from langchain_experimental.agents import create_csv_agent
import constants


os.environ["OPENAI_API_KEY"] = constants.openai_api_key

# oi_data_df = pd.read_csv("enriched_oi_data.csv")

query = sys.argv[1]

llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0)
agent_executor = create_csv_agent(
    llm,
    ["D:\\Work\\AI\\LangChain\\Output\\All_stock_open_interest_data.csv", "D:\\Work\\AI\\LangChain\\Output\\All_stocks_ltp_ohlc_data.csv"],
    agent_type="openai-tools",
    verbose=True,
    allow_dangerous_code=True
)


print(agent_executor.run(query))
