from flask import Flask, render_template, request, flash, redirect, url_for
import csv
import redis
import secrets

app = Flask(__name__)
app.secret_key = secrets.token_hex(16)  # Generate a random secret key

# Connect to Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)

# RediSearch index name
index_name = 'contacts'

# Clear existing data and index
redis_client.flushall()

# Create RediSearch index with fields "firstname", "lastname", "email", "position", and "company"
try:
    redis_client.execute_command('FT.CREATE', index_name, 'ON', 'HASH', 'PREFIX', '1', 'contact:',
                                'SCHEMA', 'firstname', 'TEXT', 'lastname', 'TEXT', 'email', 'TEXT', 'position', 'TEXT', 'company', 'TEXT')
except redis.exceptions.ResponseError as e:
    print(f"Error creating index: {e}")

# Function to import CSV into Redis and create RediSearch indexes
def import_csv_to_redis_search(csv_path):
    with open(csv_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)

        for idx, row in enumerate(csv_reader, start=1):
            try:
                # Extract relevant fields
                firstname = row.get('First Name', '').strip() or 'nil'
                lastname = row.get('Last Name', '').strip() or 'nil'
                email = row.get('Email Address', '').strip() or 'nil'
                position = row.get('Position', '').strip() or 'nil'
                company = row.get('Company', '').strip() or 'nil'

                # Store data in Redis hash
                redis_client.hset(f'contact:{firstname}:{lastname}', 'firstname', firstname)
                redis_client.hset(f'contact:{firstname}:{lastname}', 'lastname', lastname)
                redis_client.hset(f'contact:{firstname}:{lastname}', 'email', email)
                redis_client.hset(f'contact:{firstname}:{lastname}', 'position', position)
                redis_client.hset(f'contact:{firstname}:{lastname}', 'company', company)

                # Index data in RediSearch
                redis_client.execute_command(
                    'FT.ADD', index_name, f'contact:{firstname}:{lastname}', '1.0',
                    'FIELDS', 'firstname', firstname, 'lastname', lastname, 'email', email,
                    'position', position, 'company', company
                )
            except Exception as e:
                print(f"Error processing row {idx}: {e}")

# Import CSV and create indexes
import_csv_to_redis_search('path_to_csv.csv')  # Update with your actual file path

# Route for home page
@app.route('/')
def home():
    return render_template('index.html')

# Route for handling file upload
@app.route('/upload', methods=['POST'])
def upload():
    try:
        file = request.files['file']
        if file and file.filename.endswith('.csv'):
            file.save('uploaded_file.csv')  # Save the uploaded file
            import_csv_to_redis_search('uploaded_file.csv')  # Import data to Redis
            flash('File uploaded successfully!', 'success')
        else:
            flash('Invalid file format. Please upload a CSV file.', 'error')
    except Exception as e:
        flash(f'Error during file upload: {str(e)}', 'error')

    return redirect(url_for('home'))

# Route for search functionality
@app.route('/search', methods=['GET', 'POST'])
def search():
    if request.method == 'POST':
        search_query = request.form['search_query']
        try:
            page = int(request.args.get('page', 1))
            search_results = perform_search(search_query, page=page)
            return render_template('search_results.html', results=search_results, query=search_query, page=page)
        except redis.exceptions.ResponseError as e:
            print(f"Error during search: {e}")
            flash('Error during search. Please try again.', 'error')

    return render_template('search.html')

# Function to perform search in Redis
def perform_search(query, page=1, per_page=10):
    offset = (page - 1) * per_page
    search_results = redis_client.execute_command('FT.SEARCH', index_name, query, 'LIMIT', str(offset), str(per_page))

    # Ensure each result is a list with an even number of elements
    formatted_results = []
    for result in search_results:
        if isinstance(result, list) and len(result) % 2 == 0:
            formatted_results.append(result)

    return formatted_results

if __name__ == '__main__':
    app.run(debug=True)
