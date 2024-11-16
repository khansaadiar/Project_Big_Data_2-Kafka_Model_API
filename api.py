from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging
import os

# Set Java home environment variable
os.environ['JAVA_HOME'] = '/Library/Java/JavaVirtualMachines/adoptopenjdk-11.jdk/Contents/Home'

app = Flask(__name__)
CORS(app)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables for DataFrames
df_model1 = None
df_model2 = None
df_model3 = None
spark = None

def initialize_spark():
    """Initialize Spark session"""
    global spark
    spark = SparkSession.builder \
        .appName("ProductRecommendation") \
        .config("spark.driver.extraJavaOptions", "-Djavax.security.auth.useSubjectCredsOnly=false") \
        .config("spark.executor.extraJavaOptions", "-Djavax.security.auth.useSubjectCredsOnly=false") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    return spark

def clean_category_name(category):
    """Clean category name for display"""
    # Split by last occurrence of '|' and take the last part
    return category.split('|')[-1].strip()

def load_dataframes():
    """Load all dataframes on startup"""
    global df_model1, df_model2, df_model3, spark
    
    if spark is None:
        spark = initialize_spark()
    
    data_dir = 'data'
    try:
        df_model1 = spark.read.csv(os.path.join(data_dir, 'clustered_model_1.csv'), header=True, inferSchema=True)
        df_model2 = spark.read.csv(os.path.join(data_dir, 'clustered_model_2.csv'), header=True, inferSchema=True)
        df_model3 = spark.read.csv(os.path.join(data_dir, 'clustered_model_3.csv'), header=True, inferSchema=True)
        
        # Clean category names on load
        df_model1 = df_model1.withColumn('category', F.trim(F.col('category')))
        df_model2 = df_model2.withColumn('category', F.trim(F.col('category')))
        df_model3 = df_model3.withColumn('category', F.trim(F.col('category')))
        
        logger.info("All datasets loaded successfully")
        
        # Log available categories for debugging
        categories = [row.category for row in df_model1.select('category').distinct().collect()]
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
        categories = [row.category for row in df_model1.select('category').distinct().collect()]
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
        categories = [row.category for row in df_model1.select('category').distinct().collect()]
        logger.info(f"Available categories: {categories}")
        
        # Case-insensitive matching for main category
        df = df_model1.filter(F.lower(F.col('category')).startswith(main_category.lower()))
        
        if df.count() == 0:
            # Try partial matching if exact match fails
            df = df_model1.filter(F.lower(F.col('category')).contains(main_category.lower()))
        
        if df.count() == 0:
            logger.warning(f"No products found for category: {main_category}")
            return jsonify({'error': f'No products found for category: {main_category}'}), 404
        
        # Get top-rated products
        products = df.orderBy(F.col('rating').desc()).limit(5)
        
        result = [{
            'name': row.product_name,
            'price': float(row.discounted_price) if row.discounted_price is not None else 0.0,
            'actual_price': float(row.actual_price) if row.actual_price is not None else 0.0,
            'discount': float(row.discount_percentage) if row.discount_percentage is not None else 0.0,
            'rating': float(row.rating) if row.rating is not None else 0.0,
            'rating_count': int(row.rating_count) if row.rating_count is not None else 0,
            'description': row.about_product if row.about_product is not None else '',
            'image': row.img_link if row.img_link is not None else '',
            'category': clean_category_name(row.category)
        } for row in products.collect()]
        
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
        similar_products = df_model2.filter(F.lower(F.col('product_name')).contains(product_name)).limit(5)
        
        if similar_products.count() == 0:
            logger.warning(f"No similar products found for: {product_name}")
            return jsonify({'error': f'No similar products found for: {product_name}'}), 404
        
        result = [{
            'name': row.product_name,
            'price': float(row.discounted_price) if row.discounted_price is not None else 0.0,
            'actual_price': float(row.actual_price) if row.actual_price is not None else 0.0,
            'discount': float(row.discount_percentage) if row.discount_percentage is not None else 0.0,
            'rating': float(row.rating) if row.rating is not None else 0.0,
            'rating_count': int(row.rating_count) if row.rating_count is not None else 0,
            'description': row.about_product if row.about_product is not None else '',
            'image': row.img_link if row.img_link is not None else '',
            'category': clean_category_name(row.category)
        } for row in similar_products.collect()]
        
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
        price_filtered = df_model3.filter(
            (F.col('discounted_price') >= min_price) & 
            (F.col('discounted_price') <= max_price)
        ).orderBy(F.col('rating').desc()).limit(5)
        
        if price_filtered.count() == 0:
            logger.warning(f"No products found in price range: ${min_price:.2f} - ${max_price:.2f}")
            return jsonify({'error': f'No products found in price range: ${min_price:.2f} - ${max_price:.2f}'}), 404
        
        result = [{
            'name': row.product_name,
            'price': float(row.discounted_price) if row.discounted_price is not None else 0.0,
            'actual_price': float(row.actual_price) if row.actual_price is not None else 0.0,
            'discount': float(row.discount_percentage) if row.discount_percentage is not None else 0.0,
            'rating': float(row.rating) if row.rating is not None else 0.0,
            'rating_count': int(row.rating_count) if row.rating_count is not None else 0,
            'description': row.about_product if row.about_product is not None else '',
            'image': row.img_link if row.img_link is not None else '',
            'category': clean_category_name(row.category)
        } for row in price_filtered.collect()]
        
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
        # Get all distinct categories
        raw_categories = [row.category for row in df_model1.select('category').distinct().collect()]
        
        # Initialize category tree
        category_tree = {}
        
        # Process each category string
        for cat in raw_categories:
            # Split by pipe and clean whitespace
            parts = [part.strip() for part in cat.split('|')]
            
            # Get main category and subcategory
            if len(parts) >= 2:
                main_cat = parts[0].replace(' ', '')  # Remove spaces
                sub_cat = parts[1].replace(' ', '')   # Remove spaces
                
                # Initialize main category if not exists
                if main_cat not in category_tree:
                    category_tree[main_cat] = set()
                
                # Add subcategory if it's not empty
                if sub_cat:
                    # Clean up the subcategory
                    sub_cat = sub_cat.replace('&', '&')  # Preserve & symbol
                    sub_cat = ''.join(c if c.isalnum() or c in ['&', ','] else '' for c in sub_cat)
                    category_tree[main_cat].add(sub_cat)
        
        # Convert sets to sorted lists and create final structure
        result = {}
        for main_cat, sub_cats in sorted(category_tree.items()):
            # Clean up main category
            clean_main_cat = main_cat.replace('&', '&')  # Preserve & symbol
            clean_main_cat = ''.join(c if c.isalnum() or c == '&' else '' for c in clean_main_cat)
            
            # Add to result with cleaned and sorted subcategories
            if sub_cats:  # Only add if there are subcategories
                result[clean_main_cat] = sorted(list(sub_cats))
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error getting categories: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.before_request
def initialize():
    """Initialize the application by loading data"""
    global df_model1, df_model2, df_model3, spark
    if df_model1 is None or df_model2 is None or df_model3 is None:
        if not load_dataframes():
            logger.error("Failed to initialize application data")

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)