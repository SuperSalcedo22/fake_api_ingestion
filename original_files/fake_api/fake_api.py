from flask import Flask, jsonify, request
import csv

app = Flask(__name__)
CSV_FILE = 'fake_bookings.csv'

ALLOWED_FIELDS = [
    "booking_id",
    "check_in_date",
    "check_out_date",
    "owner_company",
    "owner_company_country"
]

def load_data():
    with open(CSV_FILE, newline='') as csvfile:
        return list(csv.DictReader(csvfile))

# Load data once at startup
bookings = load_data()

@app.route('/api/bookings')
def get_bookings():
    filters = request.args
    filtered = bookings.copy()

    # --- Filtering ---
    for key, value in filters.items():
        if key in ALLOWED_FIELDS:
            filtered = [item for item in filtered if value.lower() in item.get(key, '').lower()]

    # --- Sorting ---
    sort_by = filters.get("sort_by")
    sort_order = filters.get("sort_order", "asc").lower()
    if sort_by in ALLOWED_FIELDS:
        filtered.sort(
            key=lambda x: x.get(sort_by, "").lower(),
            reverse=(sort_order == "desc")
        )

    # --- Pagination ---
    try:
        page = int(filters.get("page", 1))
        per_page = int(filters.get("per_page", 10))
    except ValueError:
        return jsonify({"error": "Invalid pagination values"}), 400

    start = (page - 1) * per_page
    end = start + per_page
    paginated = filtered[start:end]

    return jsonify({
        "total": len(filtered),
        "page": page,
        "per_page": per_page,
        "results": paginated
    })

if __name__ == '__main__':
    app.run(debug=True)
