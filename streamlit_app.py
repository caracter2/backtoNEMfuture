import pandas as pd
import datetime
import streamlit as st
import predispatch_daily

st.title('Day-slice NEM Predispatch Predictions')

state_selected = st.selectbox(
     'State',
     ('NSW','QLD','VIC','SA','TAS'))

# market_selected = st.selectbox('Market',
#                                 ('Energy','LOWER5MIN', 'LOWER60SEC', 'LOWER6SEC', 'LOWERREG',
#                                 'RAISE5MIN','RAISE60SEC', 'RAISE6SEC', 'RAISEREG')
#                                 )

# show_future_settled = st.checkbox('Show future settled prices')

date_selection_type = st.radio(
     "What date would you like to look at?",
     ('Today', 'Specific date'))

if date_selection_type == 'Specific date':
    selected_date = st.date_input('specific date')
    
else:
    selected_date = pd.to_datetime(datetime.date.today())

start = pd.to_datetime(selected_date)
end =  selected_date + pd.Timedelta('1d')

new_fig = predispatch_daily.create_forecast_vs_actuals_chart(actuals = predispatch_daily.get_trading_price_NEMWEB(start, end),
                                       predispatch = predispatch_daily.get_predispatch_price_NEMWEB(start, end),
                                       state = state_selected)

st.plotly_chart(new_fig)

st.button("Re-fresh")