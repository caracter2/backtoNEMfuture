import pandas as pd
import datetime
import streamlit as st
import predispatch_daily

st.title('Day-slice NEM Predispatch Predictions')

state_selected = st.selectbox(
     'State',
     ('NSW','QLD','VIC','SA','TAS'))

market_selected = st.selectbox('Market',
                                ('Energy','LOWER5MIN', 'LOWER60SEC', 'LOWER6SEC', 'LOWERREG',
                                'RAISE5MIN','RAISE60SEC', 'RAISE6SEC', 'RAISEREG')
                                )

show_future_settled = st.checkbox('Show future settled prices')

date_selection_type = st.radio(
     "What date would you like to look at?",
     ('Today', 'Specific date'))

if date_selection_type == 'Specific date':
    selected_date = st.date_input('specific date')
else:
    selected_date = pd.to_datetime(datetime.date.today())
     

new_fig = predispatch_daily.create_date_fig(date = selected_date,
                                            state = state_selected,
                                            keep_all=show_future_settled,
                                            market = market_selected
                                            )
st.plotly_chart(new_fig)

st.button("Re-fresh")