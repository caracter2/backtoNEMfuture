import pandas as pd
import datetime
import streamlit as st
import predispatch_daily

st.title('Back to NEM future 🤯')


# market_selected = st.selectbox('Market',
#                                 ('Energy','LOWER5MIN', 'LOWER60SEC', 'LOWER6SEC', 'LOWERREG',
#                                 'RAISE5MIN','RAISE60SEC', 'RAISE6SEC', 'RAISEREG')
#                                 )

# show_future_settled = st.checkbox('Show future settled prices')



with st.sidebar:
    st.heading('Enter selections below')
    dateform = st.form(key='my_form')
    state_selected = dateform.selectbox(
        'State',
        ('NSW','QLD','VIC','SA','TAS'))
    date_selection_type = dateform.radio(
        "What date would you like to look at?",
        ('Yesterday','Today', 'Specific date'))
    specific_date = dateform.date_input('specific date')
    submit_button = dateform.form_submit_button(label='Load data')

    if date_selection_type == 'Specific date':
        selected_date = specific_date
    elif date_selection_type == 'Today':
        selected_date = pd.to_datetime(datetime.date.today())
        
    else:
        #(yesterday)
        selected_date = pd.to_datetime(datetime.date.today()-pd.Timedelta('1d'))

    st.write(f'Selected state: {state_selected}')
    st.write(f'Selected date: {selected_date.strftime("%d %b %Y")}')


start = pd.to_datetime(selected_date)
end =  selected_date + pd.Timedelta('1d')

new_fig = predispatch_daily.create_forecast_vs_actuals_chart(actuals = predispatch_daily.get_trading_price_NEMWEB(start, end),
                                       predispatch = predispatch_daily.get_predispatch_price_NEMWEB(start, end),
                                       state = state_selected)

st.plotly_chart(new_fig, use_container_width =True)

st.button("Refresh")