# Description: Script to answer the questions of the test using

import pandas as pd
import os
from Components import solve_questions

# Define paths
DB_PATH = os.path.join(os.path.dirname(__file__), 'DB')
RESULTS_PATH = os.path.join(os.path.dirname(__file__), 'Results')

# Variables
db_name = 'R5_Test_DWH.db'

# Execution
if __name__ == '__main__':
    if os.path.exists(os.path.join(DB_PATH, db_name)):
        q1,q2 = solve_questions.answer_questions(db_name, DB_PATH)
        if not os.path.exists(RESULTS_PATH):
            os.makedirs(RESULTS_PATH)
        q1.to_csv(os.path.join(RESULTS_PATH, 'Answer_Q1.csv'), index=False,sep=';')
        q2.to_csv(os.path.join(RESULTS_PATH, 'Answer_Q2.csv'), index=False,sep=';')