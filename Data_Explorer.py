import time
import streamlit as st
import os
import json
import polars as pl
import datetime
import math

# --- Constants ---
PAGE_SIZE = 10

st.set_page_config(
    layout="wide",
    page_icon="📊",
    page_title="Data Explorer",
    initial_sidebar_state="collapsed"
)

# --- Keep Background Styling ---
st.markdown(
    """
    <style>
        [data-testid="stAppViewContainer"] {
            background: linear-gradient(180deg, #2A5D4E 0%, #65897F 50%, #2A5D4E 100%);
        }
        [data-testid="stHeader"] {
            background: transparent;
        }
        /* Add custom styles for filter spacing if needed */
        .filter-container > div {
            margin-bottom: 10px;
        }
        /* Style multiselect */
        [data-testid="stMultiSelect"] {
            min-width: 150px;
        }
        [data-testid="stSelectbox"] {
             min-width: 150px;
        }
        [data-testid="stSlider"] {
            min-width: 200px; /* Adjust as needed */
            padding-left: 10px; /* Add some padding */
            padding-right: 10px;
        }
        /* Center align pagination */
        .pagination-container {
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 1rem;
            margin-top: 1rem;
        }
        .stButton button {
            /* Add custom button styling if desired */
        }
        /* Custom styling for state column might not be directly applicable in st.dataframe */
        /* .state-successful, .state-failed, etc. defined in old CSS won't work on st.dataframe cells */
    </style>
    """,
    unsafe_allow_html=True
)


# --- Data Loading and Metadata (largely unchanged) ---
parquet_source_path = "data.parquet"
filter_metadata_path = "filter_metadata.json"

# Default structure (remains the same)
filter_options = {
    'categories': ['All Categories'],
    'countries': ['All Countries'],
    'states': ['All States'],
    'date_ranges': [
        'All Time', 'Last Month', 'Last 6 Months', 'Last Year',
        'Last 5 Years', 'Last 10 Years'
    ],
    'sort_options': { # Define sort options clearly
        'popularity': 'Most Popular',
        'newest': 'Newest First',
        'oldest': 'Oldest First',
        'mostfunded': 'Most Funded',
        'mostbacked': 'Most Backed',
        'enddate': 'End Date'
    }
}
category_subcategory_map = {'All Categories': ['All Subcategories']}
min_max_values = {
    'pledged': {'min': 0, 'max': 1000},
    'goal': {'min': 0, 'max': 10000},
    'raised': {'min': 0, 'max': 500}
}

# Load metadata (logic remains the same)
if not os.path.exists(filter_metadata_path):
    st.error(f"Filter metadata file not found at '{filter_metadata_path}'. Please run `database_download.py` first.")
    st.stop()
else:
    try:
        with open(filter_metadata_path, 'r', encoding='utf-8') as f:
            loaded_metadata = json.load(f)

        # Validate and load categories, countries, states
        filter_options['categories'] = loaded_metadata.get('categories') or ['All Categories']
        filter_options['countries'] = loaded_metadata.get('countries') or ['All Countries']
        filter_options['states'] = loaded_metadata.get('states') or ['All States']
        filter_options['date_ranges'] = loaded_metadata.get('date_ranges', filter_options['date_ranges'])

        # Load category-subcategory map
        category_subcategory_map = loaded_metadata.get('category_subcategory_map', {'All Categories': ['All Subcategories']})
        if 'All Categories' not in category_subcategory_map:
            category_subcategory_map['All Categories'] = ['All Subcategories']
        if category_subcategory_map['All Categories'] and 'All Subcategories' not in category_subcategory_map['All Categories']:
             category_subcategory_map['All Categories'].insert(0, 'All Subcategories')

        all_subs = set(loaded_metadata.get('subcategories', ['All Subcategories']))
        all_cats_subs = set(category_subcategory_map.get('All Categories', []))
        missing_subs = all_subs - all_cats_subs
        if missing_subs:
             category_subcategory_map['All Categories'].extend(sorted(list(missing_subs)))
             category_subcategory_map['All Categories'] = sorted(list(set(category_subcategory_map['All Categories'])), key=lambda x: (x != 'All Subcategories', x))

        # Load min/max values
        loaded_min_max = loaded_metadata.get('min_max_values', {})
        min_max_values['pledged'] = loaded_min_max.get('pledged', min_max_values['pledged'])
        min_max_values['goal'] = loaded_min_max.get('goal', min_max_values['goal'])
        min_max_values['raised'] = loaded_min_max.get('raised', min_max_values['raised'])

        print("Filter metadata loaded successfully.")

    except json.JSONDecodeError:
        st.error(f"Error decoding JSON from '{filter_metadata_path}'. File might be corrupted. Using default filters.")
        # Set default min/max explicitly here if loading fails
        min_max_values = {
            'pledged': {'min': 0, 'max': 1000}, 'goal': {'min': 0, 'max': 10000}, 'raised': {'min': 0, 'max': 500}
        }
    except Exception as e:
        st.error(f"Error loading filter metadata from '{filter_metadata_path}': {e}. Using default filters.")
        # Set default min/max explicitly here if loading fails
        min_max_values = {
            'pledged': {'min': 0, 'max': 1000}, 'goal': {'min': 0, 'max': 10000}, 'raised': {'min': 0, 'max': 500}
        }


# Extract min/max for convenience
min_pledged = int(min_max_values['pledged']['min'])
max_pledged = int(min_max_values['pledged']['max'])
min_goal = int(min_max_values['goal']['min'])
max_goal = int(min_max_values['goal']['max'])
min_raised = int(min_max_values['raised']['min'])
max_raised = int(min_max_values['raised']['max'])


# --- Initialize Session State ---
# Use more descriptive keys and initialize if they don't exist
if 'current_page' not in st.session_state:
    st.session_state.current_page = 1
if 'search_query' not in st.session_state:
    st.session_state.search_query = ''
if 'selected_categories' not in st.session_state:
    st.session_state.selected_categories = ['All Categories']
if 'selected_subcategories' not in st.session_state:
    st.session_state.selected_subcategories = ['All Subcategories']
if 'selected_countries' not in st.session_state:
    st.session_state.selected_countries = ['All Countries']
if 'selected_states' not in st.session_state:
    st.session_state.selected_states = ['All States']
if 'selected_date_range' not in st.session_state:
    st.session_state.selected_date_range = 'All Time'
if 'pledged_range' not in st.session_state:
    st.session_state.pledged_range = (min_pledged, max_pledged)
if 'goal_range' not in st.session_state:
    st.session_state.goal_range = (min_goal, max_goal)
if 'raised_range' not in st.session_state:
    st.session_state.raised_range = (min_raised, max_raised)
if 'sort_order' not in st.session_state:
    st.session_state.sort_order = 'popularity' # Default sort key
if 'total_rows' not in st.session_state:
    st.session_state.total_rows = 0 # Will be calculated

# --- Base LazyFrame (logic unchanged) ---
if 'base_lf' not in st.session_state:
    if not os.path.exists(parquet_source_path):
        st.error(f"Parquet data source not found at '{parquet_source_path}'. Please ensure the file/directory exists.")
        st.stop()
    try:
        print(f"Scanning Parquet source: {parquet_source_path}")
        base_lf = pl.scan_parquet(parquet_source_path)
        print("Base LazyFrame created.")
        st.session_state.base_lf = base_lf
        schema = st.session_state.base_lf.collect_schema()
        print("Schema:", schema)
        if len(schema) == 0:
             st.error(f"Loaded data from '{parquet_source_path}' has no columns.")
             st.stop()
        if len(schema.names()) != len(set(schema.names())):
             st.error(f"Parquet source '{parquet_source_path}' contains duplicate column names. Please clean the source data.")
             from collections import Counter
             counts = Counter(schema.names())
             duplicates = [name for name, count in counts.items() if count > 1]
             st.error(f"Duplicate columns found: {duplicates}")
             st.stop()
    except Exception as e:
        st.error(f"Error scanning Parquet or initial processing: {e}")
        if hasattr(e, 'context'): st.error(f"Context: {e.context()}")
        st.stop()

# --- Filtering and Sorting Logic (adapted for new session state keys) ---
def apply_filters_and_sort(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Applies filters and sorting based on session state."""
    column_names = lf.collect_schema().names()

    # 1. Text Search
    search_term = st.session_state.search_query
    if search_term:
        search_cols = ['Project Name', 'Creator', 'Category', 'Subcategory']
        valid_search_cols = [col for col in search_cols if col in column_names]
        if valid_search_cols:
            search_expr = None
            for col in valid_search_cols:
                 current_expr = pl.col(col).cast(pl.Utf8).str.contains(f"(?i){search_term}")
                 if search_expr is None: search_expr = current_expr
                 else: search_expr = search_expr | current_expr
            if search_expr is not None: lf = lf.filter(search_expr)

    # 2. Categorical Filters
    if 'Category' in column_names and st.session_state.selected_categories != ['All Categories']:
        lf = lf.filter(pl.col('Category').is_in(st.session_state.selected_categories))
    if 'Subcategory' in column_names and st.session_state.selected_subcategories != ['All Subcategories']:
        lf = lf.filter(pl.col('Subcategory').is_in(st.session_state.selected_subcategories))
    if 'Country' in column_names and st.session_state.selected_countries != ['All Countries']:
        lf = lf.filter(pl.col('Country').is_in(st.session_state.selected_countries))
    if 'State' in column_names and st.session_state.selected_states != ['All States']:
        lf = lf.filter(pl.col('State').cast(pl.Utf8).str.to_lowercase().is_in([s.lower() for s in st.session_state.selected_states]))

    # 3. Range Filters
    pledged_min, pledged_max = st.session_state.pledged_range
    goal_min, goal_max = st.session_state.goal_range
    raised_min, raised_max = st.session_state.raised_range

    if 'Raw Pledged' in column_names:
        lf = lf.filter((pl.col('Raw Pledged') >= pledged_min) & (pl.col('Raw Pledged') <= pledged_max))
    if 'Raw Goal' in column_names:
        lf = lf.filter((pl.col('Raw Goal') >= goal_min) & (pl.col('Raw Goal') <= goal_max))
    if 'Raw Raised' in column_names:
        lf = lf.filter((pl.col('Raw Raised') >= raised_min) & (pl.col('Raw Raised') <= raised_max))

    # 4. Date Filter
    date_filter = st.session_state.selected_date_range
    if date_filter != 'All Time' and 'Raw Date' in column_names:
        now = datetime.datetime.now()
        compare_date = None
        if date_filter == 'Last Month': compare_date = now - datetime.timedelta(days=30)
        elif date_filter == 'Last 6 Months': compare_date = now - datetime.timedelta(days=182)
        elif date_filter == 'Last Year': compare_date = now - datetime.timedelta(days=365)
        elif date_filter == 'Last 5 Years': compare_date = now - datetime.timedelta(days=5*365)
        elif date_filter == 'Last 10 Years': compare_date = now - datetime.timedelta(days=10*365)

        if compare_date:
             lf = lf.with_columns(pl.col("Raw Date").cast(pl.Datetime, strict=False).alias("Raw Date_dt"))
             lf = lf.filter(pl.col('Raw Date_dt') >= compare_date).drop("Raw Date_dt")

    # 5. Sorting
    sort_order_key = st.session_state.sort_order
    sort_descending = True
    sort_col = 'Popularity Score' # Default for 'popularity'

    if sort_order_key == 'newest': sort_col, sort_descending = 'Raw Date', True
    elif sort_order_key == 'oldest': sort_col, sort_descending = 'Raw Date', False
    elif sort_order_key == 'mostfunded': sort_col, sort_descending = 'Raw Pledged', True
    elif sort_order_key == 'mostbacked': sort_col, sort_descending = 'Backer Count', True
    elif sort_order_key == 'enddate': sort_col, sort_descending = 'Raw Deadline', True # Assuming latest ending first

    if sort_col in column_names:
        lf = lf.sort(sort_col, descending=sort_descending, nulls_last=True)
    else:
        print(f"Warning: Sort column '{sort_col}' not found.")

    return lf

# --- Callback Functions ---
def reset_filters():
    st.session_state.search_query = ''
    st.session_state.selected_categories = ['All Categories']
    st.session_state.selected_subcategories = ['All Subcategories']
    st.session_state.selected_countries = ['All Countries']
    st.session_state.selected_states = ['All States']
    st.session_state.selected_date_range = 'All Time'
    st.session_state.pledged_range = (min_pledged, max_pledged)
    st.session_state.goal_range = (min_goal, max_goal)
    st.session_state.raised_range = (min_raised, max_raised)
    st.session_state.sort_order = 'popularity'
    st.session_state.current_page = 1 # Reset page on filter reset

def update_subcategories():
    """Update subcategory options and selection based on selected categories."""
    selected_cats = st.session_state.selected_categories_widget # Use widget key directly
    current_sub_selection = set(st.session_state.selected_subcategories) # Current state

    available_subcategories = set()
    is_all_categories_selected = 'All Categories' in selected_cats or not selected_cats

    if is_all_categories_selected:
        available_subcategories.update(category_subcategory_map.get('All Categories', []) or [])
    else:
        available_subcategories.add('All Subcategories') # Always include
        for cat in selected_cats:
            for subcat in category_subcategory_map.get(cat, []):
                if subcat != 'All Subcategories':
                    available_subcategories.add(subcat)

    # Check if current selection is still valid
    reset_subs = False
    for sub in current_sub_selection:
        if sub != 'All Subcategories' and sub not in available_subcategories:
            reset_subs = True
            break

    if reset_subs:
        st.session_state.selected_subcategories = ['All Subcategories']
    elif not current_sub_selection: # If was empty, set to All
        st.session_state.selected_subcategories = ['All Subcategories']
    else:
        # Keep current selection if it's valid
        st.session_state.selected_subcategories = list(current_sub_selection)

    # Reset page when filters change
    st.session_state.current_page = 1


def on_filter_change():
    # Reset page when any filter changes
    st.session_state.current_page = 1
    # Subcategory update is handled separately if categories change
    # Other widgets update session state directly via their key

def go_to_page(page_num):
    st.session_state.current_page = page_num

def next_page():
    total_pages = math.ceil(st.session_state.total_rows / PAGE_SIZE) if PAGE_SIZE > 0 else 1
    if st.session_state.current_page < total_pages:
        st.session_state.current_page += 1

def prev_page():
    if st.session_state.current_page > 1:
        st.session_state.current_page -= 1


# --- Main App Logic ---

st.title("📊 Data Explorer")

# --- Filter Controls ---
st.markdown("### Filter Projects")

filter_col1, filter_col2, filter_col3, filter_col_reset = st.columns([3, 3, 3, 1])

with filter_col1:
    st.text_input(
        "Search",
        key='search_query',
        placeholder="Search Project Name, Creator...",
        on_change=on_filter_change
    )
    st.selectbox(
        "Sort By",
        options=list(filter_options['sort_options'].keys()),
        format_func=lambda key: filter_options['sort_options'][key],
        key='sort_order',
        on_change=on_filter_change
    )

with filter_col2:
    st.multiselect(
        "Categories",
        options=filter_options['categories'],
        key='selected_categories_widget', # Use a different key for widget to trigger callback
        default=st.session_state.selected_categories,
        on_change=update_subcategories # Special callback for categories
    )
    # Update the main state from the widget state after callback potential change
    st.session_state.selected_categories = st.session_state.selected_categories_widget


    # Determine available subcategories based on selected categories
    available_subcats = set()
    if 'All Categories' in st.session_state.selected_categories or not st.session_state.selected_categories:
        available_subcats = set(category_subcategory_map.get('All Categories', ['All Subcategories']))
    else:
        available_subcats.add('All Subcategories')
        for cat in st.session_state.selected_categories:
            for sub in category_subcategory_map.get(cat, []):
                 available_subcats.add(sub)
    sorted_available_subcats = sorted(list(available_subcats), key=lambda x: (x != 'All Subcategories', x))

    st.multiselect(
        "Subcategories",
        options=sorted_available_subcats,
        key='selected_subcategories', # Directly bind to state, reset handled by category callback
        # Default is handled by the category callback logic setting the state key
        on_change=on_filter_change
    )


with filter_col3:
    st.multiselect(
        "Countries",
        options=filter_options['countries'],
        key='selected_countries',
        default=st.session_state.selected_countries,
        on_change=on_filter_change
    )
    st.multiselect(
        "States",
        options=filter_options['states'],
        key='selected_states',
        default=st.session_state.selected_states,
        on_change=on_filter_change
    )


with filter_col_reset:
    st.button("Reset All", on_click=reset_filters, use_container_width=True, type="secondary")
    st.selectbox(
        "Date Range",
        options=filter_options['date_ranges'],
        key='selected_date_range',
        on_change=on_filter_change
    )


# --- Range Sliders ---
st.markdown("### Adjust Ranges")
range_col1, range_col2, range_col3 = st.columns(3)

with range_col1:
    st.slider(
        f"Pledged Amount ($ {min_pledged:,} - {max_pledged:,})",
        min_value=min_pledged,
        max_value=max_pledged,
        value=st.session_state.pledged_range,
        key='pledged_range',
        on_change=on_filter_change
    )

with range_col2:
     st.slider(
        f"Goal Amount ($ {min_goal:,} - {max_goal:,})",
        min_value=min_goal,
        max_value=max_goal,
        value=st.session_state.goal_range,
        key='goal_range',
        on_change=on_filter_change
    )

with range_col3:
     st.slider(
        f"Percentage Raised (% {min_raised:,} - {max_raised:,})",
        min_value=min_raised,
        max_value=max_raised,
        value=st.session_state.raised_range,
        key='raised_range',
        on_change=on_filter_change
    )

st.divider()


# --- Apply Filters and Fetch Data ---

# 1. Apply filters and sorting
print(f"Applying filters/sort based on state: {st.session_state}")
if 'base_lf' not in st.session_state:
     st.error("Base LazyFrame not found in session state. Please reload.")
     st.stop()

filtered_lf = apply_filters_and_sort(st.session_state.base_lf)

# 2. Calculate total rows
try:
    print("Calculating total rows...")
    start_count_time = time.time()
    total_rows_result = filtered_lf.select(pl.len()).collect(streaming=True)
    st.session_state.total_rows = total_rows_result.item() if total_rows_result is not None and total_rows_result.height > 0 else 0
    count_duration = time.time() - start_count_time
    print(f"Total rows calculated: {st.session_state.total_rows} (took {count_duration:.2f}s)")
except Exception as e:
    st.error(f"Error calculating total rows: {e}")
    st.session_state.total_rows = 0

# 3. Calculate pagination details
total_pages = math.ceil(st.session_state.total_rows / PAGE_SIZE) if PAGE_SIZE > 0 else 1
# Clamp page number *after* filter changes and *before* fetching
st.session_state.current_page = max(1, min(st.session_state.current_page, total_pages if total_pages > 0 else 1))
offset = (st.session_state.current_page - 1) * PAGE_SIZE

# 4. Fetch data for the current page
df_page = pl.DataFrame() # Default to empty DF
if st.session_state.total_rows > 0 and offset < st.session_state.total_rows and PAGE_SIZE > 0:
    try:
        print(f"Fetching page {st.session_state.current_page} (offset: {offset}, limit: {PAGE_SIZE})...")
        start_fetch_time = time.time()
        # Select only columns needed for display + potentially link column raw URL if different
        # Note: st.dataframe handles large datasets well, but selecting fewer cols upstream is good practice
        visible_columns = ['Project Name', 'Creator', 'Pledged Amount', 'Link', 'Country', 'State']
        # Add raw columns needed for config if different from display format
        required_raw_cols = ['Raw Pledged', 'Link'] # Example: Need raw link for LinkColumn
        cols_to_fetch = list(set(visible_columns + required_raw_cols))
        # Ensure columns exist in the LazyFrame before selecting
        fetchable_cols = [col for col in cols_to_fetch if col in filtered_lf.columns]

        if fetchable_cols:
             df_page = filtered_lf.select(fetchable_cols).slice(offset, PAGE_SIZE).collect(streaming=True)
             fetch_duration = time.time() - start_fetch_time
             print(f"Page data fetched: {len(df_page)} rows (took {fetch_duration:.2f}s)")
        else:
             st.warning("None of the required display columns found in the data. Displaying empty table.")

    except Exception as e:
        st.error(f"Error fetching data for page {st.session_state.current_page}: {e}")
else:
     print("No rows to fetch for the current page/filters.")

# --- Display Data Table ---
st.markdown(f"#### Displaying Page {st.session_state.current_page} of {total_pages} ({st.session_state.total_rows:,} total projects)")

# Define column configuration for st.dataframe
column_config = {
    "Project Name": st.column_config.TextColumn(width="medium"),
    "Creator": st.column_config.TextColumn(width="small"),
    "Pledged Amount": st.column_config.NumberColumn(
        label="Pledged ($)", # Nicer label
        format="$ %d", # Format as integer currency
        # We can't easily get min/max for *this page*, so omit for now
        width="small",
    ),
     "Link": st.column_config.LinkColumn(
         label="Project Link",
         # Assumes the 'Link' column contains the valid URL
         display_text="Visit Project", # Text shown in the link
         width="medium"
     ),
     "Country": st.column_config.TextColumn(width="small"),
     "State": st.column_config.TextColumn(
         label="Status", # Rename for clarity
         width="small"
         # Conditional formatting based on value (e.g., color) is NOT supported here.
     )
}

# Ensure only columns present in the fetched df_page are in the config and order
existing_cols_in_page = df_page.columns
valid_column_config = {k: v for k, v in column_config.items() if k in existing_cols_in_page}
# Define desired order, filtering by existing columns
desired_order = ['Project Name', 'Creator', 'Pledged Amount', 'State', 'Country', 'Link']
valid_column_order = [col for col in desired_order if col in existing_cols_in_page]


if not df_page.is_empty():
    st.dataframe(
        df_page,
        column_order=valid_column_order if valid_column_order else None, # Apply order if valid cols exist
        column_config=valid_column_config,
        hide_index=True,
        use_container_width=True
    )
elif st.session_state.total_rows > 0:
     st.warning("Could not fetch data for the current page.")
else:
     st.info("No projects match the current filters.")


# --- Pagination Controls ---
if total_pages > 1:
    st.markdown('<div class="pagination-container">', unsafe_allow_html=True)
    col_prev, col_page, col_next = st.columns([1, 2, 1])
    with col_prev:
        st.button("⬅️ Previous", on_click=prev_page, disabled=st.session_state.current_page <= 1, use_container_width=True)
    with col_page:
         # Simple page number display
         # st.write(f"Page {st.session_state.current_page} of {total_pages}")
         # Or a number input for jumping (might need clamping/validation)
         st.number_input(
             "Page",
             min_value=1,
             max_value=total_pages,
             value = st.session_state.current_page, # Use value, not default
             key="page_jumper", # Unique key
             on_change=lambda: go_to_page(st.session_state.page_jumper), # Use lambda to pass value
             label_visibility="collapsed"
             )
    with col_next:
        st.button("Next ➡️", on_click=next_page, disabled=st.session_state.current_page >= total_pages, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)


# --- REMOVED Component Rendering Call ---
# component_return_value = table_component(...)
# st.session_state.kickstarter_state_value = component_return_value ...

# Optional Debug Info
# st.divider()
# st.caption("Debug Info:")
# st.json({k: v for k, v in st.session_state.items() if k != 'base_lf'}) # Avoid showing large LF object