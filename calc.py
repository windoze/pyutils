import random

def generate_questions(num_questions):
    questions = []
    for i in range(num_questions):
        num1 = random.randint(2, 99)
        num2 = random.randint(2, 99)
        operator = random.choice(['+', '-', 'x', '÷'])
        
        if operator == '+':
            answer = num1 + num2
        elif operator == '-':
            num1, num2 = max(num1, num2), min(num1, num2)
            answer = num1 - num2
        elif operator == 'x':
            answer = num1 * num2
        elif operator == '÷':
            num1 = num2 * random.randint(1, 39)
            answer = num1 // num2
        
        question = f"{num1} {operator} {num2} ="
        questions.append((question, answer))
    
    return questions

def print_questions_in_columns(questions):
    column_width = max(len(q[0]) for q in questions) + 40
    num_columns = 2
    num_questions = len(questions)
    num_per_column = (num_questions + num_columns - 1) // num_columns

    for i in range(num_per_column):
        for j in range(num_columns):
            index = i + j * num_per_column
            if index < num_questions:
                question, _ = questions[index]
                print(question.ljust(column_width), end='')
        print()

def main():
    num_questions = int(input("How many questions do you want to generate? "))
    questions = generate_questions(num_questions)
    # print("\nHere are the questions in two columns:")
    print_questions_in_columns(questions)

if __name__ == "__main__":
    main()
