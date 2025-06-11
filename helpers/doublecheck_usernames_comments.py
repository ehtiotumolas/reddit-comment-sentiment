import pandas as pd
import glob


# Loads all of the user-parquet files into a single DataFrame.
user_files = glob.glob("data/users_parquet/*.parquet")
df_users = pd.concat([pd.read_parquet(path) for path in user_files], ignore_index=True)

# Loads the comments parquet into another DataFrame.
df_comments = pd.read_parquet("data/comments_parquet/all_comments_merged.parquet")

# Inspects column names to find which column holds the username.
print("Users columns:", df_users.columns.tolist())
print("Comments columns:", df_comments.columns.tolist())

set_users = set(df_users["username"].dropna().unique())
set_comments = set(df_comments["username"].dropna().unique())

# Finds users with zero comments:
users_without_comments = set_users - set_comments

# Finds commenters not in the user list:
commenters_not_users = set_comments - set_users

print(f"Number of users with no comments: {len(users_without_comments)}")
print(f"Number of commenters not in the user list: {len(commenters_not_users)}")

# 8. If you want to see a small sample of each:
print("Sample users with no comments:", list(users_without_comments)[:10])
print("Sample commenters not in users:", list(commenters_not_users)[:10])
