import os
import tempfile
import zipfile
import snowflake.connector
import boto3
from botocore.exceptions import ClientError
import streamlit as st
import io

def convert_procedure(sql_code):
    """
    Converts an MS SQL Server stored procedure to a Snowflake-compatible stored procedure using AWS Bedrock's Claude model.
    """
    client = boto3.client("bedrock-runtime", region_name="us-east-1")
    model_id = "anthropic.claude-3-5-sonnet-20240620-v1:0"

    user_message = f"""Human: You are a highly skilled SQL and Snowflake expert. Your task is to convert an MS SQL Server stored procedure into a Snowflake-compatible SQL stored procedure using SQL language for procedure conversion. Don't use JavaScript. Write high-quality Snowflake SQL code for the conversion.

    Ensure the conversion handles all necessary syntax differences between MS SQL Server and Snowflake. Review your output thoroughly to confirm there are no syntax errors or functional discrepancies.
    
    Here is the MS SQL Server stored procedure code:
    <task>
    {sql_code}
    </task>
    
    Assistant:
    """
    
    conversation = [
        {
            "role": "user",
            "content": [{"text": user_message}],
        }
    ]

    try:
        response = client.converse(
            modelId=model_id,
            messages=conversation,
            inferenceConfig={"maxTokens": 2048, "stopSequences": ["\n\nHuman:"], "temperature": 0, "topP": 1},
            additionalModelRequestFields={"top_k": 250}
        )

        response_text = response["output"]["message"]["content"][0]["text"]
        start_index = response_text.find("```sql")
        end_index = response_text.find("```", start_index + 6)
        
        if start_index != -1 and end_index != -1:
            sql_code_block = response_text[start_index + 6:end_index].strip()
            return sql_code_block
        else:
            return None

    except (ClientError, Exception) as e:
        print(f"ERROR: Can't invoke '{model_id}'. Reason: {e}")
        return None

def procedure_error_rtry(connection, sql_code, error):
    """
    Attempts to resolve an error in creating a Snowflake stored procedure using AWS Bedrock's Claude model.
    """
    client = boto3.client("bedrock-runtime", region_name="us-east-1")
    model_id = "anthropic.claude-3-5-sonnet-20240620-v1:0"

    user_message = f"""Human: You are a highly skilled SQL and Snowflake expert. Your task is to resolve an issue with a Snowflake stored procedure.
    You will be provided with a Snowflake stored procedure and its respective error. Try resolving the code issue based on the error message.

    Review your output thoroughly to confirm there are no syntax errors or functional discrepancies.
    
    Here is the Snowflake stored procedure code:
    <task>
    {sql_code}
    </task>
    <error>
    {error}
    </error>
    
    Assistant:
    """
    
    conversation = [
        {
            "role": "user",
            "content": [{"text": user_message}],
        }
    ]

    try:
        response = client.converse(
            modelId=model_id,
            messages=conversation,
            inferenceConfig={"maxTokens": 2048, "stopSequences": ["\n\nHuman:"], "temperature": 0, "topP": 1},
            additionalModelRequestFields={"top_k": 250}
        )

        response_text = response["output"]["message"]["content"][0]["text"]
        start_index = response_text.find("```sql")
        end_index = response_text.find("```", start_index + 6)
        
        if start_index != -1 and end_index != -1:
            sql_code_block = response_text[start_index + 6:end_index].strip()
            return sql_code_block
        else:
            return None

    except (ClientError, Exception) as e:
        print(f"ERROR: Can't invoke '{model_id}'. Reason: {e}")
        return None

def create_stored_procedure_in_snowflake(connection, sql_code):
    """
    Executes the given SQL code to create a stored procedure in Snowflake.
    """
    try:
        cursor = connection.cursor()
        cursor.execute(sql_code)
        return True
    except snowflake.connector.errors.ProgrammingError as e:
        corrected_sql_code = procedure_error_rtry(connection, sql_code, str(e))
        if corrected_sql_code:
            try:
                cursor.execute(corrected_sql_code)
                return True
            except snowflake.connector.errors.ProgrammingError:
                return False
    finally:
        cursor.close()
    return False

def process_sql_code(sql_code):
    """ Converts SQL code and returns the result. """
    converted_code = convert_procedure(sql_code)
    return converted_code

def create_zip_file(converted_files):
    """Creates a zip file containing all converted SQL files."""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        for file_name, converted_code in converted_files:
            zip_file.writestr(file_name, converted_code)
    zip_buffer.seek(0)  # Move to the beginning of the BytesIO buffer
    return zip_buffer

def main():
    st.title("MS SQL Server to Snowflake Procedure Converter")
    
    # File uploader
    uploaded_files = st.file_uploader("Upload SQL files", type=["sql"], accept_multiple_files=True)

    if st.button("Convert"):
        if uploaded_files:
            converted_files = []
            created_successfully = []
            not_created_files = []

            # Snowflake connection
            try:
                snowflake_connection = snowflake.connector.connect(
                    user='azadk',
                    password='Azad@@1786',
                    account='oq43111.ap-south-1.aws',
                    warehouse='COMPUTE_WH',
                    database='GAM_PDW',
                    schema='DBO',
                    role='DATA_MIGRATION_ROLE'
                )

                for uploaded_file in uploaded_files:
                    sql_code = uploaded_file.read().decode("utf-8")
                    converted_code = process_sql_code(sql_code)

                    if converted_code:
                        converted_files.append((os.path.basename(uploaded_file.name), converted_code))

                        if create_stored_procedure_in_snowflake(snowflake_connection, converted_code):
                            created_successfully.append(os.path.basename(uploaded_file.name))
                        else:
                            not_created_files.append(os.path.basename(uploaded_file.name))
                    else:
                        not_created_files.append(os.path.basename(uploaded_file.name))

                # Display results
                st.success("Conversion Complete!")
                
                # Files successfully created in Snowflake
                st.subheader("Successfully Created Procedures in Snowflake:")
                st.write(created_successfully)

                # Files that were not created
                st.subheader("Not Created Procedures:")
                st.write(not_created_files)

                # Create a zip file for download
                zip_file = create_zip_file(converted_files)
                st.download_button(label="Download All Converted Files", data=zip_file, file_name="converted_procedures.zip", mime='application/zip')

            except Exception as e:
                st.error(f"Failed to connect to Snowflake: {e}")
            finally:
                snowflake_connection.close()
        else:
            st.warning("Please upload at least one SQL file.")

if __name__ == "__main__":
    main()




































# import os
# import tempfile
# import snowflake.connector
# import boto3
# from botocore.exceptions import ClientError
# import streamlit as st

# def convert_procedure(sql_code):
#     """
#     Converts an MS SQL Server stored procedure to a Snowflake-compatible stored procedure using AWS Bedrock's Claude model.
#     """
#     client = boto3.client("bedrock-runtime", region_name="us-east-1")
#     model_id = "anthropic.claude-3-5-sonnet-20240620-v1:0"

#     user_message = f"""Human: You are a highly skilled SQL and Snowflake expert. Your task is to convert an MS SQL Server stored procedure into a Snowflake-compatible SQL stored procedure using SQL language for procedure conversion. Don't use JavaScript. Write high-quality Snowflake SQL code for the conversion.

#     Ensure the conversion handles all necessary syntax differences between MS SQL Server and Snowflake. Review your output thoroughly to confirm there are no syntax errors or functional discrepancies.
    
#     Here is the MS SQL Server stored procedure code:
#     <task>
#     {sql_code}
#     </task>
    
#     Assistant:
#     """
    
#     conversation = [
#         {
#             "role": "user",
#             "content": [{"text": user_message}],
#         }
#     ]

#     try:
#         response = client.converse(
#             modelId=model_id,
#             messages=conversation,
#             inferenceConfig={"maxTokens": 2048, "stopSequences": ["\n\nHuman:"], "temperature": 0, "topP": 1},
#             additionalModelRequestFields={"top_k": 250}
#         )

#         response_text = response["output"]["message"]["content"][0]["text"]
#         start_index = response_text.find("```sql")
#         end_index = response_text.find("```", start_index + 6)
        
#         if start_index != -1 and end_index != -1:
#             sql_code_block = response_text[start_index + 6:end_index].strip()
#             return sql_code_block
#         else:
#             return None

#     except (ClientError, Exception) as e:
#         print(f"ERROR: Can't invoke '{model_id}'. Reason: {e}")
#         return None

# def procedure_error_rtry(connection, sql_code, error):
#     """
#     Attempts to resolve an error in creating a Snowflake stored procedure using AWS Bedrock's Claude model.
#     """
#     client = boto3.client("bedrock-runtime", region_name="us-east-1")
#     model_id = "anthropic.claude-3-5-sonnet-20240620-v1:0"

#     user_message = f"""Human: You are a highly skilled SQL and Snowflake expert. Your task is to resolve an issue with a Snowflake stored procedure.
#     You will be provided with a Snowflake stored procedure and its respective error. Try resolving the code issue based on the error message.

#     Review your output thoroughly to confirm there are no syntax errors or functional discrepancies.
    
#     Here is the Snowflake stored procedure code:
#     <task>
#     {sql_code}
#     </task>
#     <error>
#     {error}
#     </error>
    
#     Assistant:
#     """
    
#     conversation = [
#         {
#             "role": "user",
#             "content": [{"text": user_message}],
#         }
#     ]

#     try:
#         response = client.converse(
#             modelId=model_id,
#             messages=conversation,
#             inferenceConfig={"maxTokens": 2048, "stopSequences": ["\n\nHuman:"], "temperature": 0, "topP": 1},
#             additionalModelRequestFields={"top_k": 250}
#         )

#         response_text = response["output"]["message"]["content"][0]["text"]
#         start_index = response_text.find("```sql")
#         end_index = response_text.find("```", start_index + 6)
        
#         if start_index != -1 and end_index != -1:
#             sql_code_block = response_text[start_index + 6:end_index].strip()
#             return sql_code_block
#         else:
#             return None

#     except (ClientError, Exception) as e:
#         print(f"ERROR: Can't invoke '{model_id}'. Reason: {e}")
#         return None

# def create_stored_procedure_in_snowflake(connection, sql_code):
#     """
#     Executes the given SQL code to create a stored procedure in Snowflake.
#     """
#     try:
#         cursor = connection.cursor()
#         cursor.execute(sql_code)
#         return True
#     except snowflake.connector.errors.ProgrammingError as e:
#         corrected_sql_code = procedure_error_rtry(connection, sql_code, str(e))
#         if corrected_sql_code:
#             try:
#                 cursor.execute(corrected_sql_code)
#                 return True
#             except snowflake.connector.errors.ProgrammingError:
#                 return False
#     finally:
#         cursor.close()
#     return False

# def process_sql_code(sql_code):
#     """ Converts SQL code and returns the result. """
#     converted_code = convert_procedure(sql_code)
#     return converted_code

# def main():
#     st.title("MS SQL Server to Snowflake Procedure Converter")
    
#     # File uploader
#     uploaded_files = st.file_uploader("Upload SQL files", type=["sql"], accept_multiple_files=True)

#     if st.button("Convert"):
#         if uploaded_files:
#             converted_files = []
#             created_successfully = []
#             not_created_files = []

#             # Snowflake connection
#             try:
#                 snowflake_connection = snowflake.connector.connect(
#                     user='azadk',
#                     password='Azad@@1786',
#                     account='oq43111.ap-south-1.aws',
#                     warehouse='COMPUTE_WH',
#                     database='GAM_PDW',
#                     schema='DBO',
#                     role='DATA_MIGRATION_ROLE'
#                 )

#                 for uploaded_file in uploaded_files:
#                     sql_code = uploaded_file.read().decode("utf-8")
#                     converted_code = process_sql_code(sql_code)

#                     if converted_code:
#                         # Save the converted code and its filename for download
#                         converted_files.append((os.path.basename(uploaded_file.name), converted_code))

#                         if create_stored_procedure_in_snowflake(snowflake_connection, converted_code):
#                             created_successfully.append(os.path.basename(uploaded_file.name))
#                         else:
#                             not_created_files.append(os.path.basename(uploaded_file.name))
#                     else:
#                         not_created_files.append(os.path.basename(uploaded_file.name))

#                 # Display results
#                 st.success("Conversion Complete!")
                
#                 # Files successfully created in Snowflake
#                 st.subheader("Successfully Created Procedures in Snowflake:")
#                 st.write(created_successfully)

#                 # Files that were not created
#                 st.subheader("Not Created Procedures:")
#                 st.write(not_created_files)

#                 st.subheader("Converted Files:")
#                 for file_name, converted_code in converted_files:
#                     st.download_button(label=f"Download {file_name}", data=converted_code, file_name=file_name, mime='text/plain')

#             except Exception as e:
#                 st.error(f"Failed to connect to Snowflake: {e}")
#             finally:
#                 snowflake_connection.close()
#         else:
#             st.warning("Please upload at least one SQL file.")

# if __name__ == "__main__":
#     main()


























# import os
# import tempfile
# import snowflake.connector
# import boto3
# from botocore.exceptions import ClientError
# import streamlit as st

# def convert_procedure(sql_code):
#     """
#     Converts an MS SQL Server stored procedure to a Snowflake-compatible stored procedure using AWS Bedrock's Claude model.
#     """
#     client = boto3.client("bedrock-runtime", region_name="us-east-1")
#     model_id = "anthropic.claude-3-5-sonnet-20240620-v1:0"

#     user_message = f"""Human: You are a highly skilled SQL and Snowflake expert. Your task is to convert an MS SQL Server stored procedure into a Snowflake-compatible SQL stored procedure using SQL language for procedure conversion. Don't use JavaScript. Write high-quality Snowflake SQL code for the conversion.

#     Ensure the conversion handles all necessary syntax differences between MS SQL Server and Snowflake. Review your output thoroughly to confirm there are no syntax errors or functional discrepancies.
    
#     Here is the MS SQL Server stored procedure code:
#     <task>
#     {sql_code}
#     </task>
    
#     Assistant:
#     """
    
#     conversation = [
#         {
#             "role": "user",
#             "content": [{"text": user_message}],
#         }
#     ]

#     try:
#         response = client.converse(
#             modelId=model_id,
#             messages=conversation,
#             inferenceConfig={"maxTokens": 2048, "stopSequences": ["\n\nHuman:"], "temperature": 0, "topP": 1},
#             additionalModelRequestFields={"top_k": 250}
#         )

#         response_text = response["output"]["message"]["content"][0]["text"]
#         start_index = response_text.find("```sql")
#         end_index = response_text.find("```", start_index + 6)
        
#         if start_index != -1 and end_index != -1:
#             sql_code_block = response_text[start_index + 6:end_index].strip()
#             return sql_code_block
#         else:
#             return None

#     except (ClientError, Exception) as e:
#         print(f"ERROR: Can't invoke '{model_id}'. Reason: {e}")
#         return None

# def procedure_error_rtry(connection, sql_code, error):
#     """
#     Attempts to resolve an error in creating a Snowflake stored procedure using AWS Bedrock's Claude model.
#     """
#     client = boto3.client("bedrock-runtime", region_name="us-east-1")
#     model_id = "anthropic.claude-3-5-sonnet-20240620-v1:0"

#     user_message = f"""Human: You are a highly skilled SQL and Snowflake expert. Your task is to resolve an issue with a Snowflake stored procedure.
#     You will be provided with a Snowflake stored procedure and its respective error. Try resolving the code issue based on the error message.

#     Review your output thoroughly to confirm there are no syntax errors or functional discrepancies.
    
#     Here is the Snowflake stored procedure code:
#     <task>
#     {sql_code}
#     </task>
#     <error>
#     {error}
#     </error>
    
#     Assistant:
#     """
    
#     conversation = [
#         {
#             "role": "user",
#             "content": [{"text": user_message}],
#         }
#     ]

#     try:
#         response = client.converse(
#             modelId=model_id,
#             messages=conversation,
#             inferenceConfig={"maxTokens": 2048, "stopSequences": ["\n\nHuman:"], "temperature": 0, "topP": 1},
#             additionalModelRequestFields={"top_k": 250}
#         )

#         response_text = response["output"]["message"]["content"][0]["text"]
#         start_index = response_text.find("```sql")
#         end_index = response_text.find("```", start_index + 6)
        
#         if start_index != -1 and end_index != -1:
#             sql_code_block = response_text[start_index + 6:end_index].strip()
#             return sql_code_block
#         else:
#             return None

#     except (ClientError, Exception) as e:
#         print(f"ERROR: Can't invoke '{model_id}'. Reason: {e}")
#         return None

# def create_stored_procedure_in_snowflake(connection, sql_code):
#     """
#     Executes the given SQL code to create a stored procedure in Snowflake.
#     """
#     try:
#         cursor = connection.cursor()
#         cursor.execute(sql_code)
#         return True
#     except snowflake.connector.errors.ProgrammingError as e:
#         corrected_sql_code = procedure_error_rtry(connection, sql_code, str(e))
#         if corrected_sql_code:
#             try:
#                 cursor.execute(corrected_sql_code)
#                 return True
#             except snowflake.connector.errors.ProgrammingError:
#                 return False
#     finally:
#         cursor.close()
#     return False

# def process_sql_code(sql_code):
#     """ Converts SQL code and returns the result. """
#     converted_code = convert_procedure(sql_code)
#     return converted_code

# def main():
#     st.title("MS SQL Server to Snowflake Procedure Converter")
    
#     # File uploader
#     uploaded_files = st.file_uploader("Upload SQL files", type=["sql"], accept_multiple_files=True)

#     if st.button("Convert"):
#         if uploaded_files:
#             converted_files = []
#             not_converted_files = []

#             # Snowflake connection
#             try:
#                 snowflake_connection = snowflake.connector.connect(
#                     user='azadk',
#                     password='Azad@@1786',
#                     account='oq43111.ap-south-1.aws',
#                     warehouse='COMPUTE_WH',
#                     database='GAM_PDW',
#                     schema='DBO',
#                     role='DATA_MIGRATION_ROLE'
#                 )

#                 for uploaded_file in uploaded_files:
#                     sql_code = uploaded_file.read().decode("utf-8")
#                     converted_code = process_sql_code(sql_code)

#                     if converted_code:
#                         # Save the converted code and its filename for download
#                         converted_files.append((os.path.basename(uploaded_file.name), converted_code))

#                         if create_stored_procedure_in_snowflake(snowflake_connection, converted_code):
#                             # Mark as converted successfully
#                             pass
#                         else:
#                             not_converted_files.append(uploaded_file.name)
#                     else:
#                         not_converted_files.append(uploaded_file.name)

#                 # Display results
#                 st.success("Conversion Complete!")
#                 st.subheader("Converted Files:")
#                 for file_name, converted_code in converted_files:
#                     st.download_button(label=f"Download {file_name}", data=converted_code, file_name=file_name, mime='text/plain')
                    
#                 st.subheader("Not Converted Files:")
#                 st.write(not_converted_files)

#             except Exception as e:
#                 st.error(f"Failed to connect to Snowflake: {e}")
#             finally:
#                 snowflake_connection.close()
#         else:
#             st.warning("Please upload at least one SQL file.")

# if __name__ == "__main__":
#     main()
