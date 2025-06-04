import numpy as np
import pandas as pd

N_RULES = 170  # например, 170 правил (можно увеличить, если нужно)
N_ROWS = 10000  # количество строк

rule_columns = [f'rule_{i}' for i in range(1, N_RULES + 1)]

np.random.seed(42)
synthetic_data = pd.DataFrame(
    np.random.choice([0, 1], size=(N_ROWS, N_RULES)),
    columns=rule_columns
)

synthetic_data['APPLICATID'] = np.arange(N_ROWS)
synthetic_data['APPLICATDATE'] = pd.date_range(start='2023-01-01', periods=N_ROWS, freq='D')

cols = ['APPLICATID', 'APPLICATDATE'] + rule_columns
synthetic_data = synthetic_data[cols]


# Генерируем случайные веса для правил
weights = np.random.uniform(0.5, 2.0, N_RULES)

# Вычисляем исходный скоринг
raw_score = synthetic_data[rule_columns].values @ weights

# Масштабируем скоринг в диапазон 100-999
min_score, max_score = 999, 100
scaled_score = ((raw_score - raw_score.min()) / (raw_score.max() - raw_score.min())) * (max_score - min_score) + min_score
synthetic_data['SCORE'] = scaled_score.round().astype(int)


# Генерация синтетических данных для перформа (метрики доходности и дефолта)
np.random.seed(42)


synthetic_perform = pd.DataFrame({
    'Id': np.arange(10000),
    'DateTime': pd.date_range('2023-01-01', periods=10000, freq='H'),
    'LoanSerialNumber': np.random.choice([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15], 10000, 
                        p=[0.88, 0.058, 0.025, 0.012, 0.0073, 0.004, 0.0023, 0.0015, 0.0012375, 0.0012375, 0.0012375, 0.0012375, 0.0012375, 0.0012375, 0.0012375, 0.0012375]),
    'IssuedDate': pd.date_range('2023-01-01', periods=10000, freq='H'),
    'UserId': [f'USER-{i}' for i in range(10000)],
    'MainDebt': np.random.uniform(1000, 30000, 10000),
    'Status': np.random.choice(
        ['BorrowerRejected','Cancellation','Filling','LoanIssued','LoanReturned','Rejected'], 
        10000, 
        p=[0.0008, 0.2752, 0.0, 0.007, 0.131, 0.586]
    ),
    'Scor_Scorista': np.random.uniform(300, 850, 10000),
    'Scor_Scorista_Issued': np.random.uniform(300, 850, 10000),
    'FPD_30': 0, 
    'Surplus_60': 0.0,
    'ltv_60': 0.0,
    'DPD_20_75':0.0
})

mask_loan_issued = synthetic_perform['Status'].isin(['LoanIssued','LoanReturned'])
num_loan_issued = mask_loan_issued.sum()

synthetic_perform.loc[mask_loan_issued, 'FPD_30'] = np.random.choice([0, 1], num_loan_issued, p=[0.85, 0.15])
synthetic_perform.loc[mask_loan_issued, 'Surplus_60'] = np.random.uniform(0, 1.5, num_loan_issued)
synthetic_perform.loc[mask_loan_issued, 'ltv_60'] = np.random.uniform(0.5, 1.2, num_loan_issued)
synthetic_perform.loc[mask_loan_issued, 'DPD_20_75'] = np.random.choice([0, 1], num_loan_issued, p=[0.81, 0.19])
synthetic_perform['IsIssued'] = np.where(synthetic_perform['Status'].isin(['LoanIssued','LoanReturned']), 1, 0)


synthetic_perform['FPD_30'] = np.where(synthetic_perform['IsIssued']==1, synthetic_perform['FPD_30'], np.nan)
synthetic_perform['Surplus_60'] = np.where(synthetic_perform['IsIssued']==1, synthetic_perform['Surplus_60'], np.nan)
synthetic_perform['ltv_60'] = np.where(synthetic_perform['IsIssued']==1, synthetic_perform['ltv_60'], np.nan)
synthetic_perform['DPD_20_75'] = np.where(synthetic_perform['IsIssued']==1, synthetic_perform['DPD_20_75'], np.nan)

synthetic_perform.to_csv('scoring/synthetic_perform.csv',sep=',')
synthetic_data.to_csv('scoring/synthetic_data.csv',sep=',')