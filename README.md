# Analytics Engineer - Take Home Assignment

**Background**

Insurance Co is looking to optimize its online insurance buying process and use of marketing resources. You are provided with an Excel dataset containing CRM data concerning term life and critical illness insurance applications. Your objective is to prepare a dashboard, analyze the data, and present insights to tackle two primary questions:

* How can we enhance the rate of policy purchases?

* What should the acquisition cost target be for the marketing team?

<br>

> ### The buying process unfolds across these stages:
>
> 1. Customer initiates an application.
> 2. Customer submits a completed application.
> 3. Insurance Co evaluates the application and decides on approval.
> 4. Customer finalizes the policy purchase.
It's important to note that customers may drop out at any stage. Application data will be incomplete if the customer drops prior to submitting a completed application (Step 2).

#### Instructions

● Please state any assumptions that you make about the case.

● If your application continues to an interview, you will be asked follow-up questions about
this case in your interviews.

● Please spend no more than 2 hours putting together your response.

<br>

#### Evaluation Criteria

* Clarity and effectiveness of the dashboard visualizations / explanations to convey your insights to non-technical audiences (marketing, ops, finance).

* Relevance of insights gained from the analysis.
Note: We are primarily looking to understand how you communicate your results and how well you translate business problems into data solutions. We will have time to dive deeper on technical skills in the technical interview towards the end of the process.

<br>

#### Case Questions

1. Create a dashboard or analysis using Excel that addresses the following:

    1.a How could we improve the conversion rate from application initiation to policy
    purchase? You are welcome to communicate these results however you would like, but at minimum the reader should be able to understand what the conversion rates look like and where the biggest drop offs are occuring.

    1.b Based on 1a and the monthly premiums and policy lengths, what would be a reasonable acquisition cost target to give to the marketing team?

2. If you were given more time to work on this problem, what would you like to do? Is there any other data you would like to collect?
Given the time limit, remember that we’re looking for you to display good communication skills and address the core business problems. Your dashboard / analysis should make it clear to the reader the answers to the questions above. You can use any combination of data, visualizations, text etc. to communicate your message!


### Setup 

The csv file used here (analytics_engineer_take_home_assignment_v1.csv), it was converted to .csv previously.

1. Opem the Databricks WEB IDE using this [LINK](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/938604926274821/300697287821923/1676615757215603/latest.html)


2. Get the database on Github repository:
```
%py
!git clone https://github.com/murillo-ro-silva/case_policyme.git
```

3. Create the database and table structure:
```
DROP DATABASE policyme CASCADE;
CREATE DATABASE policyme;
CREATE TABLE policyme.insurance_events
  USING csv
  OPTIONS (path "file:/databricks/driver/case_policyme/analytics_engineer_take_home_assignment_v1.csv", header "true");
```
