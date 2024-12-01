import sys
import pandas as pd
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from langchain.llms import OpenAI
import constants


os.environ["OPENAI_API_KEY"] = constants.openai_api_key

oi_data_df = pd.read_csv("enriched_oi_data.csv")

llm = OpenAI(model="gpt-4", temperature=0.5)  # Use the desired model (e.g., GPT-4)

# Define a prompt template for asking the model about the data
prompt_template = sys.argv[1]

# Initialize the prompt template
prompt = PromptTemplate(input_variables=["data"], template=prompt_template)

# Create Langchain LLMChain
chain = LLMChain(llm=llm, prompt=prompt)

# Convert the DataFrame into a text representation that GPT can understand
def df_to_text(df):
    # Convert DataFrame to a string format (for example, displaying the first 10 rows for analysis)
    return df.to_string(index=False)

# Pass the DataFrame as text to Langchain and get the model's response
data_text = df_to_text(oi_data_df)
response = chain.run(data=data_text)

# Print the analysis
print(response)
