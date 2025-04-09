import re
from PyPDF2 import PdfReader
import psycopg2

PDF_PATH = 'C:/Users/pedro/Downloads/Enem-Extractor/Enem-Extractor/2023_PV_impresso_D1_CD1.pdf'

# Database connection details
DB_PARAMS = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
    "host": "localhost",
    "port": "5432"
}

# Connect to PostgreSQL
def connect_db():
    return psycopg2.connect(**DB_PARAMS)

def clean_string(s):
    # return s.replace("\t", " ").replace("  ", " ")
    return s.replace("\n", " ").replace("\t", " ").replace("  ", " ")

def extract_text(pdf_path):
    text = ""
    with open(pdf_path, "rb") as f:
        reader = PdfReader(f)
        for page in reader.pages:
            text += page.extract_text()
    return text

def extract_questions_and_alternatives(text):

    # Find the subjects
    subject_blocks = re.findall(
        r'([A-ZÇÃÕÂÊÁÉÍÓÚÜ,\s]+?)\s*Questões de (\d{2}) a (\d{2})(?!\s*\(opção)',
        text,
        re.MULTILINE
    )
    print(subject_blocks)

    subject_map = {}
    for subject, start, end in subject_blocks:
        for num in range(int(start), int(end) + 1):
            subject_map[num] = subject.strip()


    # Find all question numbers
    question_numbers = re.findall(r'QUESTÃO (\d+)', text)
    
    # Split the text where each question starts
    questions = re.split(r'QUESTÃO \d+', text)
    
    question_data = []
    for i, question in enumerate(questions[1:]):  # skip the preamble (questions[0])
        # Split the question into question text and alternatives
        question_split = re.split(r'\n[A-E]\s[A-E]\s', question)
        question_text = clean_string(question_split[0].strip())
        alternatives = [alt.strip() for alt in question_split[1:] if alt.strip()]
        
        # Optional cleanup of alternatives
        for j in range(len(alternatives)):
            alternatives[j] = clean_string(alternatives[j].split('.')[0] + '.')
        
        # Add subject
        subject = subject_map.get(int(question_numbers[i]), 'DESCONHECIDO')  # fallback to 'UNKNOWN'

        question_data.append({
            'number': int(question_numbers[i]),
            'subject': subject,
            'question': question_text,
            'alternatives': alternatives
        })
    
    return question_data

def create_tables():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS tests (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE
        );
        CREATE TABLE IF NOT EXISTS questions (
            id SERIAL PRIMARY KEY,
            test_id INTEGER REFERENCES tests(id),
            question_number INTEGER,
            subject TEXT,
            question_text TEXT
        );
        CREATE TABLE IF NOT EXISTS alternatives (
            id SERIAL PRIMARY KEY,
            question_id INTEGER REFERENCES questions(id),
            alternative_text TEXT
        );
    ''')
    conn.commit()
    cur.close()
    conn.close()

def store_data(question_data):
    conn = connect_db()
    cur = conn.cursor()
    
    # Ensure the test is in the database
    cur.execute("INSERT INTO tests (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id", ('Enem 2023',))
    test_id = cur.fetchone()
    if not test_id:
        cur.execute("SELECT id FROM tests WHERE name = %s", ('Enem 2023',))
        test_id = cur.fetchone()[0]
    else:
        test_id = test_id[0]
    
    for question in question_data:
        cur.execute(
            "INSERT INTO questions (test_id, question_number, subject, question_text) VALUES (%s, %s, %s, %s) RETURNING id",
            (test_id, question['number'], question['subject'], question['question'])
        )
        question_id = cur.fetchone()[0]
    
        for alternative in question['alternatives']:
            cur.execute(
                "INSERT INTO alternatives (question_id, alternative_text) VALUES (%s, %s)",
                (question_id, alternative)
            )
    
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    create_tables()
    text = extract_text(PDF_PATH)
    question_data = extract_questions_and_alternatives(text)
    store_data(question_data)
    print("Data extraction and storage completed!")
