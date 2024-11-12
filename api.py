from flask import Flask, request, jsonify, render_template
import pandas as pd
from flask_cors import CORS
import logging
import os

app = Flask(__name__)
CORS(app)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables for DataFrames
df_model1 = None
df_model2 = None
df_model3 = None

def clean_category_name(category):
    """Clean category name for display"""
    # Split by last occurrence of '|' and take the last part
    return category.split('|')[-1].strip()

def load_dataframes():
    """Load all dataframes on startup"""
    global df_model1, df_model2, df_model3
    
    data_dir = 'data'
    try:
        df_model1 = pd.read_csv(os.path.join(data_dir, 'clustered_model_1.csv'))
        df_model2 = pd.read_csv(os.path.join(data_dir, 'clustered_model_2.csv'))
        df_model3 = pd.read_csv(os.path.join(data_dir, 'clustered_model_3.csv'))
        
        # Clean category names on load
        df_model1['category'] = df_model1['category'].str.strip()
        df_model2['category'] = df_model2['category'].str.strip()
        df_model3['category'] = df_model3['category'].str.strip()
        
        logger.info("All datasets loaded successfully")
        
        # Log available categories for debugging
        categories = df_model1['category'].unique()
        logger.info(f"Available categories: {categories}")
        
        return True
    except Exception as e:
        logger.error(f"Error loading datasets: {e}")
        return False

@app.route('/')
def home():
    """Render the main page with dynamic categories"""
    if df_model1 is not None:
        # Get main categories (first level)
        categories = df_model1['category'].unique()
        # Clean and organize categories
        main_categories = sorted(list(set([cat.split('|')[0].strip() for cat in categories])))
        return render_template('index.html', categories=main_categories)
    return render_template('index.html', categories=[])

@app.route('/api/products', methods=['GET'])
def get_products_by_category():
    main_category = request.args.get('category', '').strip()
    
    if not main_category:
        return jsonify({'error': 'Category parameter is required'}), 400
    
    try:
        # Log the requested category and available categories
        logger.info(f"Requested category: {main_category}")
        logger.info(f"Available categories: {df_model1['category'].unique()}")
        
        # Case-insensitive matching for main category
        mask = df_model1['category'].str.lower().str.startswith(main_category.lower())
        df = df_model1[mask]
        
        if df.empty:
            # Try partial matching if exact match fails
            mask = df_model1['category'].str.lower().str.contains(main_category.lower())
            df = df_model1[mask]
        
        if df.empty:
            logger.warning(f"No products found for category: {main_category}")
            return jsonify({'error': f'No products found for category: {main_category}'}), 404
        
        # Get top-rated products
        products = df.nlargest(5, 'rating')
        
        result = products.apply(lambda row: {
            'name': row['product_name'],
            'price': float(row['discounted_price']) if pd.notnull(row['discounted_price']) else 0.0,
            'actual_price': float(row['actual_price']) if pd.notnull(row['actual_price']) else 0.0,
            'discount': float(row['discount_percentage']) if pd.notnull(row['discount_percentage']) else 0.0,
            'rating': float(row['rating']) if pd.notnull(row['rating']) else 0.0,
            'rating_count': int(row['rating_count']) if pd.notnull(row['rating_count']) else 0,
            'description': row['about_product'] if pd.notnull(row['about_product']) else '',
            'image': row['img_link'] if pd.notnull(row['img_link']) else '',
            'category': clean_category_name(row['category'])
        }, axis=1).tolist()
        
        logger.info(f"Found {len(result)} products for category: {main_category}")
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"Error processing category request: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/similar_products', methods=['GET'])
def get_similar_products():
    product_name = request.args.get('name', '').strip().lower()
    
    if not product_name:
        return jsonify({'error': 'Product name is required'}), 400
    
    try:
        # Find similar products using case-insensitive partial matching
        mask = df_model2['product_name'].str.lower().str.contains(product_name, na=False)
        similar_products = df_model2[mask].head(5)
        
        if similar_products.empty:
            logger.warning(f"No similar products found for: {product_name}")
            return jsonify({'error': f'No similar products found for: {product_name}'}), 404
        
        result = similar_products.apply(lambda row: {
            'name': row['product_name'],
            'price': float(row['discounted_price']) if pd.notnull(row['discounted_price']) else 0.0,
            'actual_price': float(row['actual_price']) if pd.notnull(row['actual_price']) else 0.0,
            'discount': float(row['discount_percentage']) if pd.notnull(row['discount_percentage']) else 0.0,
            'rating': float(row['rating']) if pd.notnull(row['rating']) else 0.0,
            'rating_count': int(row['rating_count']) if pd.notnull(row['rating_count']) else 0,
            'description': row['about_product'] if pd.notnull(row['about_product']) else '',
            'image': row['img_link'] if pd.notnull(row['img_link']) else '',
            'category': clean_category_name(row['category'])
        }, axis=1).tolist()
        
        logger.info(f"Found {len(result)} similar products for: {product_name}")
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error processing similar products request: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/products_by_price', methods=['GET'])
def get_products_by_price():
    try:
        target_price = float(request.args.get('target_price', 0))
    except ValueError:
        return jsonify({'error': 'Invalid price value'}), 400
    
    if target_price <= 0:
        return jsonify({'error': 'Price must be greater than 0'}), 400
    
    try:
        # Define price range (±10% of target price)
        min_price = target_price * 0.9
        max_price = target_price * 1.1
        
        # Filter products within price range
        mask = (df_model3['discounted_price'] >= min_price) & (df_model3['discounted_price'] <= max_price)
        price_filtered = df_model3[mask].nlargest(5, 'rating')
        
        if price_filtered.empty:
            logger.warning(f"No products found in price range: ${min_price:.2f} - ${max_price:.2f}")
            return jsonify({'error': f'No products found in price range: ${min_price:.2f} - ${max_price:.2f}'}), 404
        
        result = price_filtered.apply(lambda row: {
            'name': row['product_name'],
            'price': float(row['discounted_price']) if pd.notnull(row['discounted_price']) else 0.0,
            'actual_price': float(row['actual_price']) if pd.notnull(row['actual_price']) else 0.0,
            'discount': float(row['discount_percentage']) if pd.notnull(row['discount_percentage']) else 0.0,
            'rating': float(row['rating']) if pd.notnull(row['rating']) else 0.0,
            'rating_count': int(row['rating_count']) if pd.notnull(row['rating_count']) else 0,
            'description': row['about_product'] if pd.notnull(row['about_product']) else '',
            'image': row['img_link'] if pd.notnull(row['img_link']) else '',
            'category': clean_category_name(row['category'])
        }, axis=1).tolist()
        
        logger.info(f"Found {len(result)} products in price range: ${min_price:.2f} - ${max_price:.2f}")
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error processing price range request: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/categories', methods=['GET'])
def get_categories():
    """Get all main categories and their subcategories"""
    if df_model1 is None:
        return jsonify({'error': 'Dataset not loaded'}), 500
    
    try:
        categories = df_model1['category'].unique()
        category_tree = {}
        
        for cat in categories:
            parts = [part.strip() for part in cat.split('|')]
            main_cat = parts[0]
            sub_cat = parts[1] if len(parts) > 1 else None
            
            if main_cat not in category_tree:
                category_tree[main_cat] = set()  # Menggunakan set untuk menghindari duplikasi
            if sub_cat:
                category_tree[main_cat].add(sub_cat)
        
        # Mengkonversi set ke list dan mengurutkannya
        result = {k: sorted(list(v)) for k, v in sorted(category_tree.items())}
        
        logger.info(f"Category tree: {result}")
        
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error getting categories: {e}")
        return jsonify({'error': 'Internal server error'}), 500
    
@app.before_request
def initialize():
    """Initialize the application by loading data"""
    global df_model1, df_model2, df_model3
    if df_model1 is None or df_model2 is None or df_model3 is None:
        if not load_dataframes():
            logger.error("Failed to initialize application data")

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)