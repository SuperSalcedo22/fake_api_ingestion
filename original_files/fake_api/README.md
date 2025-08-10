# Fake API

A tiny script + CSV to fake the API of House Group Holiday Rentals.

## How to run

- Create a dedicated `venv` and install the packages listed in `requirements.txt`.
- Move your terminal to this directory and run `python3 fake_api.py`.
- This will start the API and serve it on port `5000` until you stop it with Ctrl+C.

## How to request

The API has a single endpoint `/api/bookings`. This endpoint implements pagination, sorting and some basic filtering. You can find below a few `curl` calls and their responses to get an idea of how the API works. You can start a new terminal after you got the API running and use them to test that the API is running fine.

- Bookings for a certain check-in date:

```bash
curl "http://localhost:5000/api/bookings?check_in_date=2024-10-01"

{
  "page": 1,
  "per_page": 2,
  "results": [
    {
      "booking_id": "b3c810d6-8ab9-41f5-9810-ed809d1d1c64",
      "check_in_date": "2024-06-20 22:12:04",
      "check_out_date": "2024-06-24 22:12:04",
      "owner_company": "Garcia, Hamilton and Carr",
      "owner_company_country": "USA"
    },
    {
      "booking_id": "e019d228-3fff-46dd-b54e-a98364a5399a",
      "check_in_date": "2024-12-02 01:16:23",
      "check_out_date": "2024-12-05 01:16:23",
      "owner_company": "Campos PLC",
      "owner_company_country": "France"
    }
  ],
  "total": 1000
}
```

- Bookings for a country, with pagination being used:

```bash
curl "http://localhost:5000/api/bookings?owner_company_country=France&page=1&per_page=3"

{
  "page": 1,
  "per_page": 3,
  "results": [
    {
      "booking_id": "e019d228-3fff-46dd-b54e-a98364a5399a",
      "check_in_date": "2024-12-02 01:16:23",
      "check_out_date": "2024-12-05 01:16:23",
      "owner_company": "Campos PLC",
      "owner_company_country": "France"
    },
    {
      "booking_id": "b0cd14f7-5bdd-4cc1-900c-4f193b26c0ae",
      "check_in_date": "2024-04-11 03:57:51",
      "check_out_date": "2024-04-21 03:57:51",
      "owner_company": "Campos PLC",
      "owner_company_country": "France"
    },
    {
      "booking_id": "ca678537-d032-4f4e-9135-9fbba287b00d",
      "check_in_date": "2024-11-08 15:24:11",
      "check_out_date": "2024-11-11 15:24:11",
      "owner_company": "Campos PLC",
      "owner_company_country": "France"
    }
  ],
  "total": 97
}
```

- Sorted and paginated:

```bash
curl "http://localhost:5000/api/bookings?sort_by=check_out_date&sort_order=desc&page=1&per_page=3"

{
  "page": 1,
  "per_page": 3,
  "results": [
    {
      "booking_id": "007d0909-1dc2-4a0d-bb3f-a925321bd09b",
      "check_in_date": "2024-12-29 22:39:18",
      "check_out_date": "2024-12-31 22:39:18",
      "owner_company": "Campos PLC",
      "owner_company_country": "France"
    },
    {
      "booking_id": "bffccb7b-f3a8-40bc-9142-4f6964a3e44a",
      "check_in_date": "2024-12-29 21:11:07",
      "check_out_date": "2024-12-31 21:11:07",
      "owner_company": "Faulkner-Howard",
      "owner_company_country": "UK"
    },
    {
      "booking_id": "f71fefb6-38ef-4b9a-b5a6-08014417b91f",
      "check_in_date": "2024-12-28 20:12:36",
      "check_out_date": "2024-12-31 20:12:36",
      "owner_company": "Jones, Jefferson and Rivera",
      "owner_company_country": "USA"
    }
  ],
  "total": 1000
}
```
