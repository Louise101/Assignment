import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from fpdf import FPDF

#connect to OLAP database
engine = create_engine("postgresql+psycopg2://louisefinlayson:test_pass@localhost:5432/HF_OLAP2")

# Query Data from OLAP cube
query = """
SELECT
    "Registered_GP_Practice" AS gp_practice,
    "GP_area_deprevity_score_overall" AS deprivation_score,
    "Deprevity_catagory_overall" AS deprivation_category,
    "Patient_ID" AS patient_id,
    "Tec_or_No" AS tec_or_no
FROM olap_pat_gp;
"""
data = pd.read_sql(query, engine)

# Data Cleaning to read TEC status
data['tec_or_no'] = data['tec_or_no'].str.strip().str.lower()


# Aggregate data
deprivation_summary = data.groupby('deprivation_category').agg(
    total_patients=('patient_id', 'count'),
    tec_subscribers=('tec_or_no', lambda x: (x == 'tec').sum())
).reset_index()

# Caclulate TEC subscription rate within each category
deprivation_summary['tec_subscription_rate'] = (
    deprivation_summary['tec_subscribers'] / deprivation_summary['total_patients']
)

# Calculate average subscription rate
avg_subscription_rate = deprivation_summary['tec_subscription_rate'].mean()

# Visualization
sns.set(style='whitegrid')
plt.figure(figsize=(10, 6))

sns.barplot(
    data=deprivation_summary,
    x='deprivation_category',
    y='tec_subscription_rate',
    palette='viridis'
)
plt.axhline(avg_subscription_rate, color='red', linestyle='--', label='Average Rate')
plt.title('TEC Subscription Rate by GP Deprivation Category')
plt.xlabel('Deprivation Category (1 = lowest deprivation, 5 = highest deprivation)')
plt.ylabel('TEC Subscription Rate')
plt.legend()
plt.tight_layout()
plt.savefig('tec_subscription_rate_by_deprivation.png')
#plt.show()

#Deprivation score averages calculation

all_patients_avg_deprivation = data['deprivation_score'].mean()

tec_patients_avg_deprivation = data.loc[
    data['tec_or_no'] == 'tec', 'deprivation_score'
].mean()

non_tec_patients_avg_deprivation = data.loc[
    data['tec_or_no'] != 'tec', 'deprivation_score'
].mean()


#Visualisation bar chart
average_deprivation_scores = {
    'All Patients': all_patients_avg_deprivation,
    'TEC Subscribers': tec_patients_avg_deprivation,
    'Non-TEC Subscribers': non_tec_patients_avg_deprivation
}

plt.figure(figsize=(10, 6))
plt.bar(
    average_deprivation_scores.keys(),
    average_deprivation_scores.values(),
    color=['blue', 'green', 'red'],
    alpha=0.7
)
plt.title('Average Deprivation Scores', fontsize=16)
plt.ylabel('Average Deprivation Score', fontsize=14)
plt.xlabel('Patient Group', fontsize=14)
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
plt.tight_layout()

# Save the chart
plt.savefig('average_deprivation_scores.png')
#plt.show()



# Summary by GP practice
gp_summary = data.groupby(['gp_practice', 'deprivation_score', 'deprivation_category']).agg(
    total_patients=('patient_id', 'count'),
    tec_subscribers=('tec_or_no', lambda x: (x == 'tec').sum())
).reset_index()

# TEC subscription rate by GP practice
gp_summary['tec_subscription_rate'] = (
    gp_summary['tec_subscribers'] / gp_summary['total_patients'])



# TEC subscription rates by GP practice visualisation
plt.figure(figsize=(12, 8))
sns.scatterplot(
    data=gp_summary,
    x='deprivation_score',
    y='tec_subscription_rate',
    size='total_patients',
    hue='deprivation_category',
    palette='viridis',
    sizes=(40, 400),
    alpha=0.7
)
plt.title('TEC Subscription Rates vs GP Deprivation Scores')
plt.xlabel('Deprivation Score')
plt.ylabel('TEC Subscription Rate')
plt.legend(title='Deprivation Category', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.savefig('gp_subscription_vs_deprivation.png')
#plt.show()


# create PDF of results
class PDFReport(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 12)
        self.cell(0, 10, 'TEC Subscriptions and GP Area Deprivation Study', 0, 1, 'C')

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

    def chapter_title(self, title):
        self.set_font('Arial', 'B', 12)
        self.cell(0, 10, title, 0, 1, 'L')
        self.ln(5)

    def chapter_body(self, text):
        self.set_font('Arial', '', 12)
        self.multi_cell(0, 10, text)
        self.ln()

# Create PDF
pdf = PDFReport()
pdf.add_page()


#Add TEC subscription rate  by deprivation catagoty results and visulaisation
pdf.chapter_title('TEC subscription by deprivation category')
for _, row in deprivation_summary.iterrows():
    pdf.cell(
        0, 10,
        f"Category {row['deprivation_category']}: "
        f"Total Patients = {row['total_patients']}, "
        f"TEC Subscribers = {row['tec_subscribers']}, "
        f"Subscription Rate = {row['tec_subscription_rate']:.2%}",
        0, 1
    )

pdf.image('tec_subscription_rate_by_deprivation.png', x=10, y=120, w=180)

#add average deprivation core results and visulisation 
pdf.add_page()
pdf.chapter_title('Average deprivation score')
pdf.chapter_body(f"""
- Average deprivation score for all patients: {all_patients_avg_deprivation:.2f}
- Average deprivation score for TEC subscribers: {tec_patients_avg_deprivation:.2f}
- Average deprivation score for non-subscribers: {non_tec_patients_avg_deprivation:.2f}
""")

pdf.image('average_deprivation_scores.png', x=40, y=120, w=130)



# Add TEC subscription by GP practice results and visualisation
pdf.add_page()
pdf.chapter_title('TEC subscription rate by GP practice')

for _, row in gp_summary.iterrows():
    pdf.cell(
        0, 10,
        f"{row['gp_practice']}, "
        f"Category: {row['deprivation_category']}, "
        f"Total Patients: {row['total_patients']}, "
        f"Subscription Rate: {row['tec_subscription_rate']:.2%}",
        0, 1
    )


pdf.image('gp_subscription_vs_deprivation.png', x=10, y=100, w=180)



# Save PDF to file
pdf.output('TEC_Deprivation_GP_Analysis.pdf')

print("Analysis complete. Results saved to 'TEC_Deprivation_GP_Analysis.pdf'.")