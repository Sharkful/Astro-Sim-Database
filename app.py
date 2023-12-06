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
        st.session_state['subhalo_query_df'] = df
        return df
    except Exception as e:
        st.error(f"An error occurred: {e}")
    finally:
        connection.close()

def filtered_query_subhalo_data(pos_x_range, pos_y_range, pos_z_range, vel_x_range, vel_y_range, vel_z_range, spin_x_range, spin_y_range, spin_z_range, mass_range, mass_half_rad_range):
    # Create a database connection
    connection = create_db_connection()

    # SQL query to find the matching subhalo data
    query = f"""
    SELECT * FROM Subhalos
    WHERE 
        SubhaloPos_X BETWEEN %s AND %s AND
        SubhaloPos_Y BETWEEN %s AND %s AND
        SubhaloPos_Z BETWEEN %s AND %s AND
        SubhaloVel_X BETWEEN %s AND %s AND
        SubhaloVel_Y BETWEEN %s AND %s AND
        SubhaloVel_Z BETWEEN %s AND %s AND
        SubhaloSpin_X BETWEEN %s AND %s AND
        SubhaloSpin_Y BETWEEN %s AND %s AND
        SubhaloSpin_Z BETWEEN %s AND %s AND
        SubhaloMass BETWEEN %s AND %s AND
        SubhaloMassInHalfRad BETWEEN %s AND %s;
    """
    
    # Execute the query
    try:
        df = pd.read_sql(query, connection, params=[*pos_x_range, *pos_y_range, *pos_z_range, *vel_x_range, *vel_y_range, *vel_z_range, *spin_x_range, *spin_y_range, *spin_z_range, *mass_range, *mass_half_rad_range])
        st.session_state['subhalo_query_df'] = df
        return df
    except Exception as e:
        st.error(f"An error occurred: {e}")
    finally:
        connection.close()

def filtered_query_parameter_data(
    pos_x_range_p, pos_y_range_p, pos_z_range_p, vel_x_range_p, vel_y_range_p, vel_z_range_p, spin_x_range_p, spin_y_range_p, spin_z_range_p, mass_range_p,
    pos_x_range_s, pos_y_range_s, pos_z_range_s, vel_x_range_s, vel_y_range_s, vel_z_range_s, spin_x_range_s, spin_y_range_s, spin_z_range_s, mass_range_s
):
    # Create a database connection
    connection = create_db_connection()

    # SQL query to find the matching subhalo data
    query = f"""
    SELECT * FROM MergerOrbitalParameters
    WHERE
        primary_mass BETWEEN %s AND %s AND
        primary_pos_x BETWEEN %s AND %s AND
        primary_pos_y BETWEEN %s AND %s AND
        primary_pos_z BETWEEN %s AND %s AND
        primary_vel_x BETWEEN %s AND %s AND
        primary_vel_y BETWEEN %s AND %s AND
        primary_vel_z BETWEEN %s AND %s AND
        primary_spin_x BETWEEN %s AND %s AND
        primary_spin_y BETWEEN %s AND %s AND
        primary_spin_z BETWEEN %s AND %s AND
        secondary_mass BETWEEN %s AND %s AND
        secondary_pos_x BETWEEN %s AND %s AND
        secondary_pos_y BETWEEN %s AND %s AND
        secondary_pos_z BETWEEN %s AND %s AND
        secondary_vel_x BETWEEN %s AND %s AND
        secondary_vel_y BETWEEN %s AND %s AND
        secondary_vel_z BETWEEN %s AND %s AND
        secondary_spin_x BETWEEN %s AND %s AND
        secondary_spin_y BETWEEN %s AND %s AND
        secondary_spin_z BETWEEN %s AND %s;
    """

    # Execute the query
    try:
        df = pd.read_sql(query, connection, params=[*mass_range_p, *pos_x_range_p, *pos_y_range_p, *pos_z_range_p, *vel_x_range_p, *vel_y_range_p, *vel_z_range_p, *spin_x_range_p, *spin_y_range_p, *spin_z_range_p, *mass_range_s, *pos_x_range_s, *pos_y_range_s, *pos_z_range_s, *vel_x_range_s, *vel_y_range_s, *vel_z_range_s, *spin_x_range_s, *spin_y_range_s, *spin_z_range_s])
        st.session_state['parameter_query_df'] = df
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
    if 'subhalo_query_df' not in st.session_state:
        st.session_state['subhalo_query_df'] = None
    if 'parameter_query_df' not in st.session_state:
        st.session_state['parameter_query_df'] = None
    if 'subhalo_df' not in st.session_state:
        st.session_state['subhalo_df'] = None
    if 'selected_row_idx' not in st.session_state:
        st.session_state['selected_row_idx'] = None
    if 'image_to_show' not in st.session_state:
        st.session_state['image_to_show'] = None

    tab1, tab2, tab3 = st.tabs(["Mergers", "Subhalos", "Merger Orbital Parameters"])

    with tab1:
        with st.expander("Merger Filters"):
            # Input fields
            merger_ratio = st.slider('Merger Ratio', 0.0, 1.0, 0.5)
            child_subhalo_mass = st.slider('Child Subhalo Mass', 0.0, 100000.0, 50000.0)
            snap_num_range = st.slider('Snap Num Range', 0, 100, (25, 75))
            merger_ratio_filter = st.radio("Merger Ratio Filter", ('Less than', 'Greater than'))
            mass_filter = st.radio("Mass Filter", ('Less than', 'Greater than'))
        # Button to execute query
        if st.button('Fetch Filtered Merger Data'):
            df = query_data(merger_ratio, child_subhalo_mass, snap_num_range, merger_ratio_filter, mass_filter)
        if st.button("Fetch All Merger Data"):
            connection = create_db_connection()
            df = pd.read_sql("SELECT * FROM mergers", connection)
            st.session_state['query_df'] = df
            connection.close()
        
        if st.session_state['query_df'] is not None:
            display_df_result(st.session_state['query_df'])
            # Display the image or the not found message outside of the tab context
            st.subheader("Example Image")
            if st.session_state['image_to_show'] == "not_found":
                st.write(f"Image not found.")
            elif st.session_state['image_to_show'] is not None:
                image = Image.open(st.session_state['image_to_show'])
                st.image(image, caption=st.session_state['image_to_show'], use_column_width=True)
            if st.session_state['subhalo_df'] is not None:
                st.subheader("More Data on the Child Subhalo")
                st.dataframe(st.session_state['subhalo_df'])

    with tab2:
        with st.expander("Subhalo Filters"):
            pos_x_range = st.slider("Subhalo Position X", -100000.0, 100000.0, (-100000.0, 100000.0))
            pos_y_range = st.slider("Subhalo Position Y", -100000.0, 100000.0, (-100000.0, 100000.0))
            pos_z_range = st.slider("Subhalo Position Z", -100000.0, 100000.0, (-100000.0, 100000.0))
            vel_x_range = st.slider("Subhalo Velocity X", -500.0, 500.0, (-500.0, 500.0))
            vel_y_range = st.slider("Subhalo Velocity Y", -500.0, 500.0, (-500.0, 500.0))
            vel_z_range = st.slider("Subhalo Velocity Z", -500.0, 500.0, (-500.0, 500.0))
            spin_x_range = st.slider("Subhalo Spin X", -400.0, 400.0, (-400.0, 400.0))
            spin_y_range = st.slider("Subhalo Spin Y", -400.0, 400.0, (-400.0, 400.0))
            spin_z_range = st.slider("Subhalo Spin Z", -400.0, 400.0, (-400.0, 400.0))
            mass_range = st.slider("Subhalo Mass", 0.0, 100000.0, (0.0, 100000.0))
            mass_half_rad_range = st.slider("Subhalo Mass Half Rad", 0.0, 400.0, (0.0, 400.0))
        if st.button('Fetch Filtered Subhalo Data'):
            df = filtered_query_subhalo_data(pos_x_range, pos_y_range, pos_z_range, vel_x_range, vel_y_range, vel_z_range, spin_x_range, spin_y_range, spin_z_range, mass_range, mass_half_rad_range)
        if st.button("Fetch All Subhalo Data"):
            connection = create_db_connection()
            df = pd.read_sql("SELECT * FROM Subhalos", connection)
            st.session_state['subhalo_query_df'] = df
            connection.close()
        # Fetch all rows from the Subhalos table
        # df = pd.read_sql("SELECT * FROM Subhalos", create_db_connection())
        if st.session_state['subhalo_query_df'] is not None:
            df = st.session_state['subhalo_query_df']
            st.write("Number of matching rows:", len(df))
            st.dataframe(df)

    with tab3:
        with st.expander("Primary Parent Filters"):
            pos_x_range_p = st.slider("Subhalo_p Position X", -100000.0, 100000.0, (-100000.0, 100000.0))
            pos_y_range_p = st.slider("Subahlo_p Position Y", -100000.0, 100000.0, (-100000.0, 100000.0))
            pos_z_range_p = st.slider("Subahlo_p Position Z", -100000.0, 100000.0, (-100000.0, 100000.0))
            vel_x_range_p = st.slider("Subahlo_p Velocity X", -500.0, 500.0, (-500.0, 500.0))
            vel_y_range_p = st.slider("Subahlo_p Velocity Y", -500.0, 500.0, (-500.0, 500.0))
            vel_z_range_p = st.slider("Subahlo_p Velocity Z", -500.0, 500.0, (-500.0, 500.0))
            spin_x_range_p = st.slider("Subahlo_p Spin X", -400.0, 400.0, (-400.0, 400.0))
            spin_y_range_p = st.slider("Subahlo_p Spin Y", -400.0, 400.0, (-400.0, 400.0))
            spin_z_range_p = st.slider("Subahlo_p Spin Z", -400.0, 400.0, (-400.0, 400.0))
            mass_range_p = st.slider("Subahlo_p Mass", 0.0, 100000.0, (0.0, 100000.0))
        with st.expander("Secondary Parent Filters"):
            pos_x_range_s = st.slider("Subhalo_s Position X", -100000.0, 100000.0, (-100000.0, 100000.0))
            pos_y_range_s = st.slider("Subhalo_s Position Y", -100000.0, 100000.0, (-100000.0, 100000.0))
            pos_z_range_s = st.slider("Subhalo_s Position Z", -100000.0, 100000.0, (-100000.0, 100000.0))
            vel_x_range_s = st.slider("Subhalo_s Velocity X", -500.0, 500.0, (-500.0, 500.0))
            vel_y_range_s = st.slider("Subhalo_s Velocity Y", -500.0, 500.0, (-500.0, 500.0))
            vel_z_range_s = st.slider("Subhalo_s Velocity Z", -500.0, 500.0, (-500.0, 500.0))
            spin_x_range_s = st.slider("Subhalo_s Spin X", -400.0, 400.0, (-400.0, 400.0))
            spin_y_range_s = st.slider("Subhalo_s Spin Y", -400.0, 400.0, (-400.0, 400.0))
            spin_z_range_s = st.slider("Subhalo_s Spin Z", -400.0, 400.0, (-400.0, 400.0))
            mass_range_s = st.slider("Subhalo_s Mass", 0.0, 100000.0, (0.0, 100000.0))
        if st.button('Fetch Filtered Parameter Data'):
            df = filtered_query_parameter_data(
                pos_x_range_p, pos_y_range_p, pos_z_range_p, vel_x_range_p, vel_y_range_p, vel_z_range_p, spin_x_range_p, spin_y_range_p, spin_z_range_p, mass_range_p,
                pos_x_range_s, pos_y_range_s, pos_z_range_s, vel_x_range_s, vel_y_range_s, vel_z_range_s, spin_x_range_s, spin_y_range_s, spin_z_range_s, mass_range_s
            )
        if st.button('Fetch All Parameter Data'):
            connection = create_db_connection()
            df = pd.read_sql("SELECT * FROM MergerOrbitalParameters", connection)
            st.session_state['parameter_query_df'] = df
            connection.close()
        
        if st.session_state['parameter_query_df'] is not None:
            df = st.session_state['parameter_query_df']
            st.write("Number of matching rows:", len(df))
            st.dataframe(df)


if __name__ == "__main__":
    main()