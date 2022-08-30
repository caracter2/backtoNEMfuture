import streamlit as st
import predispatch_daily

new_fig = predispatch_daily.create_date_fig(state = 'NSW', keep_all=False)
st.plotly_chart(new_fig)