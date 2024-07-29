# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
import pandas as pd
import matplotlib.pyplot as plt

# Write directly to the app
st.title("Foreign Exchange Rate: Forecasting")


# Get the current credentials
cnx=st.connection("snowflake")
# cnx = snowflake.connector.connect(**st.secrets["connections.snowflake"])

session = cnx.session()

def build_forcast_data (currency, days):
    if currency == "INR - India":
        session.sql(f"""BEGIN CALL foreign_exchange_model_inr!FORECAST( FORECASTING_PERIODS => {days},CONFIG_OBJECT => {{'prediction_interval': 0.95}}); LET x := SQLID; CREATE or replace TABLE FORECAST_FER AS SELECT * FROM TABLE(RESULT_SCAN(:x)); END;""").collect()
        session.sql(f"""BEGIN CALL fer_model_india!FORECAST( FORECASTING_PERIODS => {days},CONFIG_OBJECT => {{'prediction_interval': 0.95}}); LET x := SQLID; CREATE or replace TABLE COMPARE_FORECAST_FER AS SELECT * FROM TABLE(RESULT_SCAN(:x)); END;""").collect()
        return session.sql("""SELECT TIME_V1 as Date, CURRENCY_VALUE FROM INDIA_FOREIGN_EXCHANGE_RATE where TIME_V1 > '2023-01-01'""").to_pandas()
    elif currency == "CAD - Canada":
        session.sql(f"""BEGIN CALL foreign_exchange_model_cad!FORECAST( FORECASTING_PERIODS => {days},CONFIG_OBJECT => {{'prediction_interval': 0.95}}); LET x := SQLID; CREATE or replace TABLE FORECAST_FER AS SELECT * FROM TABLE(RESULT_SCAN(:x)); END;""").collect()
        session.sql(f"""BEGIN CALL fer_model_canada!FORECAST( FORECASTING_PERIODS => {days},CONFIG_OBJECT => {{'prediction_interval': 0.95}}); LET x := SQLID; CREATE or replace TABLE COMPARE_FORECAST_FER AS SELECT * FROM TABLE(RESULT_SCAN(:x)); END;""").collect()
        return session.sql("""SELECT TIME_V1 as Date, CURRENCY_VALUE FROM CANADA_FOREIGN_EXCHANGE_RATE where TIME_V1 > '2023-01-01'""").to_pandas()
    elif currency == "EUR - Euro":
        session.sql(f"""BEGIN CALL foreign_exchange_model_euro!FORECAST( FORECASTING_PERIODS => {days},CONFIG_OBJECT => {{'prediction_interval': 0.95}}); LET x := SQLID; CREATE or replace TABLE FORECAST_FER AS SELECT * FROM TABLE(RESULT_SCAN(:x)); END;""").collect()
        session.sql(f"""BEGIN CALL fer_model_euro!FORECAST( FORECASTING_PERIODS => {days},CONFIG_OBJECT => {{'prediction_interval': 0.95}}); LET x := SQLID; CREATE or replace TABLE COMPARE_FORECAST_FER AS SELECT * FROM TABLE(RESULT_SCAN(:x)); END;""").collect()
        return session.sql("""SELECT TIME_V1 as Date, CURRENCY_VALUE FROM EURO_FOREIGN_EXCHANGE_RATE where TIME_V1 > '2023-01-01'""").to_pandas()
    elif currency == "JPY - Japan":
        session.sql(f"""BEGIN CALL foreign_exchange_model_jpy!FORECAST( FORECASTING_PERIODS => {days},CONFIG_OBJECT => {{'prediction_interval': 0.95}}); LET x := SQLID; CREATE or replace TABLE FORECAST_FER AS SELECT * FROM TABLE(RESULT_SCAN(:x)); END;""").collect()
        session.sql(f"""BEGIN CALL fer_model_japan!FORECAST( FORECASTING_PERIODS => {days},CONFIG_OBJECT => {{'prediction_interval': 0.95}}); LET x := SQLID; CREATE or replace TABLE COMPARE_FORECAST_FER AS SELECT * FROM TABLE(RESULT_SCAN(:x)); END;""").collect()
        return session.sql("""SELECT TIME_V1 as Date, CURRENCY_VALUE FROM JAPAN_FOREIGN_EXCHANGE_RATE where TIME_V1 > '2023-01-01'""").to_pandas()
    elif currency == "PKR - Pakistan":
        session.sql(f"""BEGIN CALL foreign_exchange_model_pak!FORECAST( FORECASTING_PERIODS => {days},CONFIG_OBJECT => {{'prediction_interval': 0.95}}); LET x := SQLID; CREATE or replace TABLE FORECAST_FER AS SELECT * FROM TABLE(RESULT_SCAN(:x)); END;""").collect()
        session.sql(f"""BEGIN CALL fer_model_pakistan!FORECAST( FORECASTING_PERIODS => {days},CONFIG_OBJECT => {{'prediction_interval': 0.95}}); LET x := SQLID; CREATE or replace TABLE COMPARE_FORECAST_FER AS SELECT * FROM TABLE(RESULT_SCAN(:x)); END;""").collect()
        return session.sql("""SELECT TIME_V1 as Date, CURRENCY_VALUE FROM PAKISTAN_FOREIGN_EXCHANGE_RATE where TIME_V1 > '2023-01-01'""").to_pandas()
    elif currency == "ILS - Israel":
        session.sql(f"""BEGIN CALL foreign_exchange_model_ils!FORECAST( FORECASTING_PERIODS => {days},CONFIG_OBJECT => {{'prediction_interval': 0.95}}); LET x := SQLID; CREATE or replace TABLE FORECAST_FER AS SELECT * FROM TABLE(RESULT_SCAN(:x)); END;""").collect()
        session.sql(f"""BEGIN CALL fer_model_israel!FORECAST( FORECASTING_PERIODS => {days},CONFIG_OBJECT => {{'prediction_interval': 0.95}}); LET x := SQLID; CREATE or replace TABLE COMPARE_FORECAST_FER AS SELECT * FROM TABLE(RESULT_SCAN(:x)); END;""").collect()
        return session.sql("""SELECT TIME_V1 as Date, CURRENCY_VALUE FROM ISRAEL_FOREIGN_EXCHANGE_RATE where TIME_V1 > '2023-01-01'""").to_pandas()


currency = st.selectbox('Select the Currency:',(
                                    'INR - India',
                                    'CAD - Canada',
                                    'EUR - Euro',
                                    'JPY - Japan',
                                    'PKR - Pakistan',
                                    'ILS - Israel'))
days = st.selectbox('Select the forecast limit(days):',(15,30,60,90))
historical_data = build_forcast_data(currency, days)
forecast_data = session.sql("""SELECT TS as Date, forecast AS CURRENCY_VALUE FROM FORECAST_FER""").to_pandas()
compare_forecast_data = session.sql("""SELECT TS as Date, forecast AS PREDICTED_VALUE FROM COMPARE_FORECAST_FER order by TS desc""").to_pandas()

actual_data=historical_data[historical_data['DATE'] <= compare_forecast_data['DATE'][0]]
actual_data =  actual_data[actual_data['DATE'] >= '2024-01-01']

plt.plot(historical_data["DATE"],historical_data['CURRENCY_VALUE'], color = "black", label='Historical')
plt.plot(forecast_data['DATE'],forecast_data['CURRENCY_VALUE'], color = "red", label='Forecast')
plt.ylabel('Price')
plt.xlabel('Date')
plt.xticks(rotation=45, ha='right')
plt.title(currency +" Forecast Chart")
plt.legend()
st.pyplot(plt.gcf())
st.dataframe(forecast_data,width=1000)
plt.clf()

plt.plot(actual_data['DATE'],actual_data['CURRENCY_VALUE'], color = "black", label='Actual')
plt.plot(compare_forecast_data["DATE"],compare_forecast_data['PREDICTED_VALUE'], color = "red", label='Predicted')

plt.ylabel('Price')
plt.xlabel('Date')
plt.xticks(rotation=45, ha='right')
plt.title(currency +" Actual-predicted Chart")
plt.legend()
st.pyplot(plt.gcf())

# for i, row in enumerate(actual_data):
# c = list(set(actual_data) & set(compare_forecast_data))
merged_table = pd.merge(actual_data, compare_forecast_data,on = 'DATE') 
# actual_data["PREDICTED"] = compare_forecast_data['PREDICTED_VALUE']
st.dataframe(merged_table,width=1000)
