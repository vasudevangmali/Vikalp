from flask import Flask, render_template, request, redirect, url_for, flash
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
DB_NAME = 'vikalp_db'
COLLECTION_NAME = 'agri_demand'

app = Flask(__name__)
app.secret_key = '4e5f2c8e4e5d9b7a8b0f3c2e4d6a1b2c'

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

FIELD_NAMES = {
'mango': 'Mango (કેરી)',
'lemon': 'Lemon (લીંબુ)',
'chiku': 'Chiku (ચીકુ)',
'guava': 'Guava (જમરૂખ)',
'saag': 'Saag (સાગ)',
'aamala': 'Aamala (આમળાં)',
'mahuda': 'Mahuda (મહુડો)',
'neem': 'Neem (લીમડો)'
}

@app.route('/')
def index():
    docs = list(collection.find({}, {'_id': 0}).limit(50))
    # print("Docs:", docs)
    return render_template('index.html', data=docs)

@app.route('/filter', methods=['GET', 'POST'])
def filter_page():
    results = []
    query = {}

    if request.method == 'POST':
        district = request.form.get('district')
        block = request.form.get('block')
        village = request.form.get('village')

        if district:
            query['District'] = {'$regex': f'^{district}$', '$options': 'i'}
        if block:
            query['Block'] = {'$regex': f'^{block}$', '$options': 'i'}
        if village:
            query['Village'] = {'$regex': f'^{village}$', '$options': 'i'}

        results = list(collection.find(query, {'_id': 0}).limit(25))

        # print("Query:", query)
        # print("Results count:", len(results))

    districts = collection.distinct('District')
    blocks = collection.distinct('Block')
    villages = collection.distinct('Village')

    return render_template('filter.html', results=results, districts=districts, blocks=blocks, villages=villages, query=query)

@app.route('/aggregate', methods=['GET', 'POST'])
def aggregate_summary():
    query = {}
    selected_district = None
    selected_block = None
    selected_state = None

    if request.method == 'POST':
        selected_district = request.form.get('district')
        selected_block = request.form.get('block')
        selected_state = request.form.get('state')

        if selected_district:
            query['District'] = {'$regex': f'^{selected_district}$', '$options': 'i'}
        if selected_block:
            query['Block'] = {'$regex': f'^{selected_block}$', '$options': 'i'}
        if selected_state:
            query['State'] = {'$regex': f'^{selected_state}$', '$options': 'i'}

    pipeline = []
    if query:
        pipeline.append({"$match": query})

    pipeline.append({
        "$group": {
            "_id": "$Year",
            "Total_Mango": {"$sum": f"${FIELD_NAMES['mango']}"},
            "Total_Lemon": {"$sum": f"${FIELD_NAMES['lemon']}"},
            "Total_Chiku": {"$sum": f"${FIELD_NAMES['chiku']}"},
            "Total_Guava": {"$sum": f"${FIELD_NAMES['guava']}"},
            "Total_Saag": {"$sum": f"${FIELD_NAMES['saag']}"},
            "Total_Aamala": {"$sum": f"${FIELD_NAMES['aamala']}"},
            "Total_Mahuda": {"$sum": f"${FIELD_NAMES['mahuda']}"},
            "Total_Neem": {"$sum": f"${FIELD_NAMES['neem']}"}
        }
    })

    pipeline.append({"$sort": {"_id": 1}})

    results = list(collection.aggregate(pipeline))

    districts = collection.distinct('District')
    blocks = collection.distinct('Block')
    states = collection.distinct('State')

    return render_template(
        'aggregration.html',
        results=results,
        districts=districts,
        blocks=blocks,
        states=states,
        selected_district=selected_district,
        selected_block=selected_block,
        selected_state=selected_state
    )

@app.route('/analysis', methods=['GET', 'POST'])
def analysis():
    query = {}
    selected_district = None
    selected_block = None

    if request.method == 'POST':
        selected_district = request.form.get('district')
        selected_block = request.form.get('block')
        if selected_district:
            query['District'] = {'$regex': f'^{selected_district}$', '$options': 'i'}
        if selected_block:
            query['Block'] = {'$regex': f'^{selected_block}$', '$options': 'i'}

    data = list(collection.find(query, {'_id': 0}))

    fruits = [col for col in data[0].keys() if col.lower() not in ['year', 'district', 'block']]    

    yearly = {}
    for row in data:
        year = row.get('Year')
        if year not in yearly:
            yearly[year] = {fruit: 0 for fruit in fruits}
        for fruit in fruits:
            try:
                yearly[year][fruit] += float(str(row.get(fruit, 0)).replace(',', ''))
            except:
                pass
            
    pipeline = []
    if query:
        pipeline.append({"$match": query})

    pipeline.append({
        "$group": {
            "_id": "$Year",
            "Mango": {"$sum": f"${FIELD_NAMES['mango']}"},
            "Lemon": {"$sum": f"${FIELD_NAMES['lemon']}"},
            "Chiku": {"$sum": f"${FIELD_NAMES['chiku']}"},
            "Guava": {"$sum": f"${FIELD_NAMES['guava']}"},
            "Saag": {"$sum": f"${FIELD_NAMES['saag']}"},
            "Aamala": {"$sum": f"${FIELD_NAMES['aamala']}"},
            "Mahuda": {"$sum": f"${FIELD_NAMES['mahuda']}"},
            "Neem": {"$sum": f"${FIELD_NAMES['neem']}"}
        }
    })
    pipeline.append({"$sort": {"_id": 1}})

    results = list(collection.aggregate(pipeline))

    years = [r['_id'] for r in results]
    mango = [r['Mango'] for r in results]
    lemon = [r['Lemon'] for r in results]
    chiku = [r['Chiku'] for r in results]
    guava = [r['Guava'] for r in results]
    total_all = [r['Mango'] + r['Lemon'] + r['Chiku'] + r['Guava'] + r['Saag'] + r['Aamala'] + r['Mahuda'] + r['Neem'] for r in results]

    total_mango = sum(mango)
    total_lemon = sum(lemon)
    total_chiku = sum(chiku)
    total_guava = sum(guava)

    districts = collection.distinct('District')
    blocks = collection.distinct('Block')

    return render_template(
        'charts.html',
        years=years,
        mango=mango,
        lemon=lemon,
        chiku=chiku,
        guava=guava,
        total_mango=total_mango,
        total_lemon=total_lemon,
        total_chiku=total_chiku,
        total_guava=total_guava,
        districts=districts,
        blocks=blocks,
        selected_district=selected_district,
        selected_block=selected_block
    )


@app.route('/top_villages/<crop>')
def top_villages(crop):
    if crop not in FIELD_NAMES:
        flash('Unknown crop')
        return redirect(url_for('index'))
    
    pipeline = [
        {"$group": {"_id": "$Village", "total": {"$sum": f"${FIELD_NAMES[crop]}"}}},
        {"$sort": {"total": -1}},
        {"$limit": 10}  
    ]

    results = list(collection.aggregate(pipeline))
    return render_template('analysis.html', results=results, title=f'Top villages for {crop}')

if __name__ == '__main__':
    app.run(debug=True)