import streamlit as st

from src.agent import get_bhujal_advice


st.set_page_config(
    page_title="Bhujal-Mitra: AI Groundwater Advisory",
    page_icon="💧",
    layout="centered",
)

st.title("Bhujal-Mitra: AI Groundwater Advisory")
st.caption("District-aware groundwater guidance for agriculture.")

with st.sidebar:
    st.header("About")
    st.write(
        "This tool combines Databricks Vector Search over groundwater policy documents "
        "with time-series groundwater forecasting to generate practical advisories."
    )
    st.info("Choose a district, ask a question, and generate advice.")

st.markdown("### Ask Your Question")

with st.form("bhujal_advice_form"):
    district_options = [
        "Pune",
    ]
    district = st.selectbox(
        "District",
        options=district_options,
        index=0,
        help="Currently configured for Pune; extend this list as new districts are onboarded.",
    )

    user_query = st.text_input(
        "Your agricultural question",
        placeholder="e.g., How can I reduce groundwater stress for summer crops this month?",
    )

    submitted = st.form_submit_button("Get Advice", type="primary")

if submitted:
    cleaned_query = user_query.strip()
    if not cleaned_query:
        st.warning("Please enter a question before submitting.")
    else:
        with st.spinner("Generating advice using policy + forecast context..."):
            try:
                advice_text = get_bhujal_advice(cleaned_query, district)
            except Exception as exc:
                st.error("Could not generate advice right now. Please try again in a moment.")
                st.exception(exc)
            else:
                st.markdown("### Advisory")
                st.markdown(advice_text)