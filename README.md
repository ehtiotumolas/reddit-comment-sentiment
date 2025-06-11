# Reddit Users and Comments Data Extraction and Storage

A suite of Python scripts for collecting Reddit user comments, performing language detection and sentiment analysis, and storing the resulting data.

## Setup

1. Create a virtual environment and activate it:

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Ensure you have the following files in the project root:
   - `reddit_credentials.json` : Reddit API credentials:
        ```json
        [{
            "client_id":"your_client_id",
            "client_secret":"your_client_secret",
            "user_agent":"your_user_agent",
            "username":"your_username",
            "password":"your_password"
        },
        {
            "client_id":"your_client_id2",
            "client_secret":"your_client_secret2",
            "user_agent":"your_user_agent2",
            "username":"your_username",
            "password":"your_password"
        },
        {
            "client_id":"..."
        }]
        ```

   - `lid.176.ftz` : FastText language detection model.

## Scripts

### `get_usernames_get_info.py`

**Description**  
Collects Reddit user comments, detects language with FastText, and performs sentiment analysis using CardiffNLP. Supports two modes:

- `single` - fetch one user every 30 seconds
- `batch`  - fetch a batch of users in parallel

**Usage**

```bash
python get_usernames_get_info.py [--mode single|batch]
```

- `--mode` : choose between `single` (default) or `batch`.

**Output**  
Parquet files saved to the `data/` directory with prefixes:

- `users_<timestamp>.parquet` : user metadata rows  
- `<username>_<timestamp>.parquet` or `batch_<count>_<timestamp>.parquet` : comment rows

**Sample**

```bash
$ python get_usernames_get_info.py --mode single
[single][#1] Processing u/exampleuser
✓ Saved 100 rows to data/exampleuser_20250611T121314.parquet
```

-----
<br>

### `counting_data.py`

**Description**  
Counts rows in Parquet files across multiple directories.

**Usage**

```bash
python counting_data.py
```

**Output**  
Prints the total number of user and comment rows, for example:

```bash
$ python counting_data.py
Total user-rows:    1,234
Total comment-rows: 56,789
```
-----
<br>

### `doublecheck_usernames_comments.py`

**Description**  
Verifies consistency between user and comment datasets by comparing usernames.

**Usage**

```bash
python doublecheck_usernames_comments.py
```

**Output**  
Prints column names, counts of mismatches, and samples:

```bash
Users columns: ['username', 'created_utc', 'link_karma', …]
Comments columns: ['username', 'comment_id', 'body_clean', …]
Number of users with no comments: 10
Number of commenters not in user list: 5
Sample users with no comments: ['user1', …]
Sample commenters not in user list: ['userA', …]
```
-----
<br>

### `mergin_all_files_deduplicated.py`

**Description**  
Merges all Parquet files in `data/` into one, removing duplicate comments.

**Usage**

```bash
python mergin_all_files_deduplicated.py
```

**Output**  
Generates `data/all_comments_merged.parquet`, for example:

```bash
$ python mergin_all_files_deduplicated.py
Loading data/comments_20250611T100000.parquet …
Loading data/comments_20250610T090000.parquet …
Concatenating…
Dropping duplicates by comment id…
Saving to data/all_comments_merged.parquet …
Done! All comments are merged and deduplicated.
```
-----
<br>

### `parquet_to_json.py`

**Description**  
Converts a Parquet file to JSON lines format.

**Usage**

```bash
python parquet_to_json.py path/to/file.parquet
```

- `path/to/file.parquet` : input Parquet file.

**Output**  
Creates a `.jsonl` file in the same directory, for example:

```bash
$ python parquet_to_json.py data/all_comments_merged.parquet
Writing JSON: 100%|█████████████████████| 50,000/50,000 rows
Done → data/all_comments_merged.jsonl
```
-----
<br>

### `split_by_language.py`

**Description**  
Splits the comments dataset into English and non-English subsets.

**Usage**

```bash
python split_by_language.py
```

**Output**  
Creates:

- `data/comments_en.parquet`  
- `data/comments_non_en.parquet`  

Sample:

```bash
$ python split_by_language.py
Loading data/all_comments.parquet …
Total records: 12,345
English comments: 6,789
Non-English comments: 5,556
Split completed. Saved English to 'data/comments_en.parquet' and non-English to 'data/comments_non_en.parquet'.
```
-----
<br>

### `testing_api_calls.py`

**Description**  
Tests Reddit API credentials by requesting OAuth tokens.

**Usage**

```bash
python testing_api_calls.py
```

**Output**  
Prints credential status:

```bash
$ python testing_api_calls.py
[1] abc123 -> OK
[2] def456 -> FAIL
```

<br>

## Notes

- Make sure to update the credentials file and model paths as needed.
- Adjust batch size and thresholds in `get_usernames_get_info.py` by editing `BATCH_SIZE` and `FT_THRESHOLD` on the script file.
