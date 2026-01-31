import pandas as pd
import json
import re
from datetime import datetime

# Load data
orders_df = pd.read_csv(
    "/Users/adityavikrammahendru/Documents/GitHub/GenAIHackathon/data/orders.csv"
)

with open(
    "/Users/adityavikrammahendru/Documents/GitHub/GenAIHackathon/data/users.json", "r"
) as f:
    users_data = json.load(f)
users_df = pd.DataFrame(users_data)

# Parse SQL file
restaurants_list = []
with open(
    "/Users/adityavikrammahendru/Documents/GitHub/GenAIHackathon/data/restaurants.sql",
    "r",
) as f:
    sql_content = f.read()

pattern = r"INSERT INTO restaurants VALUES \((\d+), '([^']+)', '([^']+)', ([\d.]+)\);"
matches = re.findall(pattern, sql_content)

for match in matches:
    restaurants_list.append(
        {
            "restaurant_id": int(match[0]),
            "restaurant_name": match[1],
            "cuisine": match[2],
            "rating": float(match[3]),
        }
    )

restaurants_df = pd.DataFrame(restaurants_list)

# Merge datasets
orders_df["order_date"] = pd.to_datetime(orders_df["order_date"], format="%d-%m-%Y")
merged_df = orders_df.merge(users_df, on="user_id", how="left")
merged_df = merged_df.rename(columns={"restaurant_name": "restaurant_name_from_orders"})
final_df = merged_df.merge(restaurants_df, on="restaurant_id", how="left")

print("=" * 80)
print("FOOD DELIVERY DATA ANALYSIS - ANSWERS TO QUESTIONS")
print("=" * 80)

# Question 1: Which city has the highest total revenue from Gold members?
print("\n1. Which city has the highest total revenue from Gold members?")
gold_city_revenue = (
    final_df[final_df["membership"] == "Gold"]
    .groupby("city")["total_amount"]
    .sum()
    .sort_values(ascending=False)
)
print(f"   Answer: {gold_city_revenue.index[0]}")
print(f"   {gold_city_revenue.to_string()}")

# Question 2: Which cuisine has the highest average order value?
print("\n2. Which cuisine has the highest average order value?")
cuisine_aov = (
    final_df.groupby("cuisine")["total_amount"].mean().sort_values(ascending=False)
)
print(f"   Answer: {cuisine_aov.index[0]}")
print(f"   {cuisine_aov.to_string()}")

# Question 3: How many distinct users placed orders worth more than ₹1000 in total?
print("\n3. How many distinct users placed orders worth more than ₹1000 in total?")
user_total = final_df.groupby("user_id")["total_amount"].sum()
users_over_1000 = (user_total > 1000).sum()
print(f"   Answer: {users_over_1000} distinct users")

if users_over_1000 < 500:
    range_answer = "< 500"
elif users_over_1000 < 1000:
    range_answer = "500 – 1000"
elif users_over_1000 < 2000:
    range_answer = "1000 – 2000"
else:
    range_answer = "> 2000"
print(f"   Range: {range_answer}")

# Question 4: Which restaurant rating range generated the highest total revenue?
print("\n4. Which restaurant rating range generated the highest total revenue?")


def get_rating_range(rating):
    if pd.isna(rating):
        return "Unknown"
    elif rating <= 3.5:
        return "3.0 – 3.5"
    elif rating <= 4.0:
        return "3.6 – 4.0"
    elif rating <= 4.5:
        return "4.1 – 4.5"
    else:
        return "4.6 – 5.0"


final_df["rating_range"] = final_df["rating"].apply(get_rating_range)
rating_revenue = (
    final_df.groupby("rating_range")["total_amount"].sum().sort_values(ascending=False)
)
print(f"   Answer: {rating_revenue.index[0]}")
print(f"   {rating_revenue.to_string()}")

# Question 5: Among Gold members, which city has the highest average order value?
print("\n5. Among Gold members, which city has the highest average order value?")
gold_city_aov = (
    final_df[final_df["membership"] == "Gold"]
    .groupby("city")["total_amount"]
    .mean()
    .sort_values(ascending=False)
)
print(f"   Answer: {gold_city_aov.index[0]}")
print(f"   {gold_city_aov.to_string()}")

# Question 6: Which cuisine has the lowest number of distinct restaurants but still contributes significant revenue?
print(
    "\n6. Which cuisine has the lowest number of distinct restaurants but still contributes significant revenue?"
)
cuisine_stats = (
    final_df.groupby("cuisine")
    .agg({"restaurant_id": "nunique", "total_amount": "sum"})
    .sort_values("restaurant_id")
)
cuisine_stats.columns = ["distinct_restaurants", "total_revenue"]
print(
    f"   Answer: {cuisine_stats.index[0]} (fewest restaurants: {cuisine_stats.iloc[0]['distinct_restaurants']}, revenue: ${cuisine_stats.iloc[0]['total_revenue']:,.2f})"
)
print(f"   {cuisine_stats.to_string()}")

# Question 7: What percentage of total orders were placed by Gold members?
print("\n7. What percentage of total orders were placed by Gold members?")
total_orders = len(final_df)
gold_orders = len(final_df[final_df["membership"] == "Gold"])
gold_percentage = round((gold_orders / total_orders) * 100)
print(f"   Answer: {gold_percentage}%")
print(f"   Total orders: {total_orders}")
print(f"   Gold member orders: {gold_orders}")

# Question 8: Which restaurant has the highest average order value but less than 20 total orders?
print(
    "\n8. Which restaurant has the highest average order value but less than 20 total orders?"
)
# Check actual column names
print(f"   Available columns: {final_df.columns.tolist()}")
# Use the correct column name (restaurant_name or restaurant_name_x from orders)
restaurant_col = (
    "restaurant_name"
    if "restaurant_name" in final_df.columns
    else "restaurant_name_from_orders"
)
restaurant_stats = (
    final_df.groupby(restaurant_col).agg({"total_amount": ["mean", "count"]}).round(2)
)
restaurant_stats.columns = ["avg_order_value", "total_orders"]
# Filter for restaurants with less than 20 orders
low_volume_restaurants = restaurant_stats[restaurant_stats["total_orders"] < 20]
if len(low_volume_restaurants) > 0:
    top_low_volume = low_volume_restaurants.sort_values(
        "avg_order_value", ascending=False
    )
    print(
        f"   Answer: {top_low_volume.index[0]} (AOV: ${top_low_volume.iloc[0]['avg_order_value']:.2f}, Orders: {int(top_low_volume.iloc[0]['total_orders'])})"
    )
    print(f"   Top 5:")
    print(f"   {top_low_volume.head().to_string()}")
else:
    print("   No restaurants found with less than 20 orders")

# Question 9: Which combination contributes the highest revenue? (Gold/Regular + cuisine)
print("\n9. Which combination contributes the highest revenue?")
combo_revenue = (
    final_df.groupby(["membership", "cuisine"])["total_amount"].sum().reset_index()
)
combo_revenue["combo"] = (
    combo_revenue["membership"] + " + " + combo_revenue["cuisine"] + " cuisine"
)
combo_revenue = combo_revenue.sort_values("total_amount", ascending=False)
print(f"   Answer: {combo_revenue.iloc[0]['combo']}")
print(f"   Revenue: ${combo_revenue.iloc[0]['total_amount']:,.2f}")
print(f"   {combo_revenue[['combo', 'total_amount']].head(10).to_string(index=False)}")

# Question 10: During which quarter is the total revenue highest?
print("\n10. During which quarter of the year is the total revenue highest?")


def get_quarter(date):
    month = date.month
    if month <= 3:
        return "Q1 (Jan–Mar)"
    elif month <= 6:
        return "Q2 (Apr–Jun)"
    elif month <= 9:
        return "Q3 (Jul–Sep)"
    else:
        return "Q4 (Oct–Dec)"


final_df["quarter"] = final_df["order_date"].apply(get_quarter)
quarter_revenue = (
    final_df.groupby("quarter")["total_amount"].sum().sort_values(ascending=False)
)
print(f"   Answer: {quarter_revenue.index[0]}")
print(f"   {quarter_revenue.to_string()}")

print("\n" + "=" * 80)
print("SUMMARY OF ANSWERS")
print("=" * 80)
print(f"1. City with highest Gold member revenue: {gold_city_revenue.index[0]}")
print(f"2. Cuisine with highest AOV: {cuisine_aov.index[0]}")
print(f"3. Users with orders > ₹1000: {users_over_1000} ({range_answer})")
print(f"4. Rating range with highest revenue: {rating_revenue.index[0]}")
print(f"5. City with highest AOV among Gold: {gold_city_aov.index[0]}")
print(f"6. Cuisine with fewest restaurants: {cuisine_stats.index[0]}")
print(f"7. Gold member order percentage: {gold_percentage}%")
if len(low_volume_restaurants) > 0:
    print(f"8. Restaurant with highest AOV (<20 orders): {top_low_volume.index[0]}")
print(f"9. Highest revenue combination: {combo_revenue.iloc[0]['combo']}")
print(f"10. Highest revenue quarter: {quarter_revenue.index[0]}")
print("=" * 80)
