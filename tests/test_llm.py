from openai import OpenAI
import sys
import os

# Mock config for test
config = {
    'openai': {
        'api_key': 'ollama'
    }
}

def sentiment_analysis(comment) -> str:
    if comment:
        try:
            client = OpenAI(
                base_url="http://host.docker.internal:11434/v1",
                api_key="ollama"
            )
            completion = client.chat.completions.create(
                model='llama3.2',
                messages = [
                    {
                    "role": "system",
                    "content": "You're a machine learning model with a task of classifying comments into POSITIVE, NEGATIVE, NEUTRAL. You are to respond with one word from the option specified above, do not add anything else."
                },
                {
                    "role": "user",
                    "content": f"Here is the comment: {comment}"
                }    ]
            )
            print(f"Full completion object: {completion}")
            return completion.choices[0].message.content
        except Exception as e:
            return f"Error: {str(e)}"
    return "Empty"

if __name__ == "__main__":
    test_comment = "I love this service! It is amazing."
    print(f"Testing comment: {test_comment}")
    result = sentiment_analysis(test_comment)
    print(f"Result: {result}")
