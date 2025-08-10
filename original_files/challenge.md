# Truvi Data Engineering Challenge

Welcome to Truvi's Data Engineering Challenge. This challenge is a small, toy sample of the kind of work we do on a daily basis. Please read through the challenge in detail. Reach out to the recruiting manager if anything is unclear.

## Context

Truvi has recently signed a contract with House Group Holiday Rentals (HG), a Property Management Company. HG operates holiday properties for multiple Owner Companies. This means the properties are strictly owned by each the Owner Companies, but HG takes care of running the rental operation for them. As part of their services, they have hired Truvi to protect the bookings that happen in the properties of the Owner Companies. Our specific agreement is that:

- All bookings that happen in their properties will be protected by Truvi (that is, if the guest does any damage to the property, we will cover for it).
- In exchange, Truvi will invoice 10GBP/14USD/12EUR (depending on the country of the Owner Company) per booking on check-out date.
- Invoicing will be done in monthly cycles. That means that each Owner Company will get one invoice, each month, with the bookings that checked out within the month.
- Besides, there's a minimum fee per Owner Company and month of 100GBP/140USD/120EUR. If there are no bookings, or the fees for the bookings that happen in a month don't add up to those amounts, the minimum fee will still be charged to the Owner Company.

To implement the invoicing of this deal, we will need to integrate with the systems of HG, fetch their booking data and process it.

## What we need

The goal of the challenge is to build a toy architecture that integrates with our customer's system, ingests their data and processes it to deliver a clean summary table for our Data Analysts and finance team colleagues. The final table must display, for each Owner Company and month, what is their revenue, in both their original currency as well as converted into GBP.

## Challenge and constraints

We would like you to set up a small system to ingest and process the data described above. The goal is to end up with a running SQL database that holds the table described in the previous section (we will simply refer to it as "the final table").

You have been delivered a folder named `fake_api`. You can check the `README.md` in that folder to run a toy fake HTTP API that mocks the customer's system. We expect you to use it. You are not expected to modify, iterate or improve this API in any way.

You should also have received a CSV named `currency_rates.csv` with currency exchange rates.

Your solution should:

- Create a SQL database. Feel free to use whatever SQL database you feel comfortable with.
- Build some way to ingest the data held in the fake API into the database. Feel free to build it as similar as to what you deliver in a production system (NOTE: we understand this is a hiring test and you have limited time and energy. We DON'T expect a PERFECT, PRODUCTION grade delivery... but the more quality you pack, the better we will appreciate your skills. It's also OK to consciously not make some bits perfect and then proactively discuss in the interview how would you build such parts in a real environment so we can learn about your ideas).
- Load the data in `currency_rates.csv` into the database to use it as part of your transformations. You can do this in any way you want, dirty and unsustainable even. We won't judge this bit other than the data being loaded. For the case, we will just assume that the real context would provide you with timely currency rates data.
- Within the database, do some transformations to deliver the final table.
- Include a way to easily print part of the final table contents.

Some guidelines:

- Please, deliver your solution via a Github repo that contains all relevant code, files, docs and other artifacts.
- We usually work on Linux systems, so we would appreciate if your solution is runnable on Linux, Mac or WSL.
- We would appreciate if your example is clear and has enough documentation so that someone can run it just by reading your submission. You can assume this person knows their way around whatever tooling you're using. You can safely assume that we're happy running this in our laptops, there's no need to bother with any sophisticated infra.
- Feel free to tackle the deployment of the SQL database in whatever way you want, but we feel using a Docker container is the simplest approach for our context. Again, you can do it differently, just keep in mind we would like to be able to execute your solution.
- Feel free to ignore anything related to authentication and security. Even if we would care about such topics in production, we won't bother for this challenge.
- Even though orchestration and monitoring are important topics that we will surely discuss with you in the interview, we're not expecting your solution to address those. It's fine if your solution gets only run once and has little output other than some terminal output.
- Feel free to add any additional documentation, explainers, human-readable bits you find relevant. If you need to make assumptions when building your solution, we encourage you to list them, for example.
