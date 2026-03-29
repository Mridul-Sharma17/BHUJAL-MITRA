import streamlit as st

from src.agent import get_bhujal_advice_bundle


st.set_page_config(
    page_title="Bhujal-Mitra: AI Groundwater Advisory",
    page_icon="💧",
    layout="wide",
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
        "Nashik",
        "Ahmednagar",
    ]
    district = st.selectbox(
        "District",
        options=district_options,
        index=0,
        help="Choose one of the supported districts for district-aware policy and forecast retrieval.",
    )

    response_language = st.selectbox(
        "Response language",
        options=["English", "Marathi"],
        index=0,
        help="The advisory will be generated in only the selected language.",
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
                payload = get_bhujal_advice_bundle(
                    cleaned_query,
                    district,
                    response_language=response_language,
                )
            except Exception as exc:
                st.error("Could not generate advice right now. Please try again in a moment.")
                st.exception(exc)
            else:
                left_col, right_col = st.columns([2, 1], gap="large")

                with left_col:
                    st.markdown(f"### Advisory ({response_language})")
                    st.markdown(payload.get("advice_text", ""))

                with right_col:
                    st.markdown("### Context Snapshot")
                    st.caption(f"District: {payload.get('district_name', district)}")

                    policy_sources = payload.get("policy_sources") or []
                    if policy_sources:
                        st.markdown("**Policy Sources Used**")
                        st.write("\n".join(f"- {item}" for item in policy_sources))
                    else:
                        st.info("No policy chunks were retrieved for this query.")

                    forecast_rows = payload.get("forecast_rows") or []
                    forecast_meta = payload.get("forecast_metadata") or {}

                    st.markdown("**Forecast Coverage**")
                    st.write(f"Rows returned: {len(forecast_rows)}")
                    if forecast_meta.get("date_column"):
                        st.write(f"Date column: {forecast_meta['date_column']}")
                    if forecast_meta.get("prediction_columns"):
                        st.write(
                            "Prediction columns: "
                            + ", ".join(str(col) for col in forecast_meta["prediction_columns"])
                        )
                    if forecast_meta.get("note"):
                        st.caption(f"Note: {forecast_meta['note']}")

                    if forecast_rows:
                        st.markdown("**Forecast Preview (top 7 rows)**")
                        st.dataframe(forecast_rows[:7], use_container_width=True)

                    diagnostics = payload.get("diagnostics") or []
                    if diagnostics:
                        st.markdown("**Diagnostics**")
                        for item in diagnostics:
                            st.caption(f"- {item}")
