import streamlit as st
import mysql.connector
import pandas as pd
from PIL import Image
import os

# Database connection function
def create_db_connection():
    return mysql.connector.connect(
        host='127.0.0.1',
        user='root',
        password='pswd',
        database="AstroSim"
    )

# Query function
def query_data(merger_ratio, child_subhalo_mass, snap_num_range, merger_ratio_filter, mass_filter):
    connection = create_db_connection()
    merger_ratio_op = ">=" if merger_ratio_filter == "Greater than" else "<="
    mass_op = ">=" if mass_filter == "Greater than" else "<="

    query = f"""
    SELECT * FROM mergers
    WHERE 
        merger_ratio {merger_ratio_op} %s AND 
        child_SubhaloMass {mass_op} %s AND 
        child_SnapNum BETWEEN %s AND %s;
    """
    df = pd.read_sql(query, connection, params=[merger_ratio, child_subhalo_mass, *snap_num_range])
    st.session_state['query_df'] = df
    connection.close()
    return df

def query_subhalo_data(child_subhalo_id_raw):
    # Create a database connection
    connection = create_db_connection()

    # SQL query to find the matching subhalo data
    query = f"SELECT * FROM Subhalos WHERE SubhaloIDRaw = {child_subhalo_id_raw}"
    
    # Execute the query
    try:
        df = pd.read_sql(query, connection)
        return df
    except Exception as e:
        st.error(f"An error occurred: {e}")
    finally:
        connection.close()

def reset_subhalo_and_image():
    st.session_state['subhalo_df'] = None
    st.session_state['image_to_show'] = None

def display_df_result(df):
    st.write("Number of matching rows:", len(df))

    st.dataframe(df)
    
    # col1, col2 = st.columns(2)

    row_idx = st.selectbox("Select a row", df.index, on_change=reset_subhalo_and_image)
    image_path = df['image_path'][row_idx]
    if os.path.exists(image_path):
        st.session_state['image_to_show'] = image_path
    else:
        st.session_state['image_to_show'] = "not_found"

    # if col2.button("View Subhalo"):
    rawId = df.iloc[row_idx]['child_SubhaloIDRaw']
    sh_df = query_subhalo_data(rawId)
    # Check if any data was returned
    if sh_df is not None and not sh_df.empty:
        st.session_state['subhalo_df'] = sh_df

# Streamlit interface
def main():
    st.title("Astronomical Database Viewer")

    if 'query_df' not in st.session_state:
        st.session_state['query_df'] = None
    if 'subhalo_df' not in st.session_state:
        st.session_state['subhalo_df'] = None
    if 'selected_row_idx' not in st.session_state:
        st.session_state['selected_row_idx'] = None
    if 'image_to_show' not in st.session_state:
        st.session_state['image_to_show'] = None

    tab1, tab2 = st.tabs(["Mergers", "Subhalos"])

    with tab1:
        # Input fields
        merger_ratio = st.sidebar.slider('Merger Ratio', 0.0, 1.0, 0.5)
        child_subhalo_mass = st.sidebar.slider('Child Subhalo Mass', 0.0, 100000.0, 50000.0)
        snap_num_range = st.sidebar.slider('Snap Num Range', 0, 100, (25, 75))

        merger_ratio_filter = st.sidebar.radio("Merger Ratio Filter", ('Less than', 'Greater than'))
        mass_filter = st.sidebar.radio("Mass Filter", ('Less than', 'Greater than'))

        # Button to execute query
        if st.sidebar.button('Fetch Data'):
            df = query_data(merger_ratio, child_subhalo_mass, snap_num_range, merger_ratio_filter, mass_filter)
        
        if st.session_state['query_df'] is not None:
            display_df_result(st.session_state['query_df'])
            # Display the image or the not found message outside of the tab context
            if st.session_state['image_to_show'] == "not_found":
                st.write(f"Image not found.")
            elif st.session_state['image_to_show'] is not None:
                image = Image.open(st.session_state['image_to_show'])
                st.image(image, caption=st.session_state['image_to_show'], use_column_width=True)
            if st.session_state['subhalo_df'] is not None:
                st.dataframe(st.session_state['subhalo_df'])

    with tab2:
        # Fetch all rows from the Subhalos table
        df = pd.read_sql("SELECT * FROM Subhalos", create_db_connection())

        # Display the DataFrame
        st.dataframe(df)


if __name__ == "__main__":
    main()