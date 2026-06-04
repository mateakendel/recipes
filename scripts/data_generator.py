from kafka import KafkaProducer
from pymongo import MongoClient
from elasticsearch import Elasticsearch
import json
import time
import random
import uuid
import os
import hashlib

KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka:9092")
MONGO_URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"
ES_HOST = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")

TOPIC_NAME = "recipe-events"
INDEX_NAME = "recipes"

NUMBER_OF_EVENTS = 3000
TARGET_RECIPE_COUNT = 90

client = MongoClient(MONGO_URI)
db = client["recepti_db"]

users = db.users
recipes = db.recipes
ratings = db.ratings
recipe_events = db.recipe_events
ml_datasets = db.ml_datasets
model_metrics = db.model_metrics

es = Elasticsearch(ES_HOST)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_HOST,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    request_timeout_ms=2000,
    api_version_auto_timeout_ms=2000,
    max_block_ms=2000
)


def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()


def wait_for_es():
    for _ in range(30):
        try:
            if es.ping():
                return True
        except Exception:
            pass

        time.sleep(1)

    return False


def create_es_index():
    if not wait_for_es():
        print("Elasticsearch not available. Recipes will still be saved to MongoDB.")
        return

    try:
        if not es.indices.exists(index=INDEX_NAME):
            es.indices.create(
                index=INDEX_NAME,
                mappings={
                    "properties": {
                        "title": {"type": "text"},
                        "ingredients": {"type": "text"},
                        "steps": {"type": "text"},
                        "category": {"type": "text"},
                        "author": {"type": "keyword"},
                        "prep_time": {"type": "integer"}
                    }
                }
            )

            print("Elasticsearch index created.")

    except Exception as e:
        print("Elasticsearch index error:", e)


def normalize_recipe(recipe):
    return {
        "title": recipe.get("title", "").lower(),
        "ingredients": " ".join(i.lower() for i in recipe.get("ingredients", [])),
        "steps": recipe.get("steps", "").lower(),
        "category": recipe.get("category", "").lower(),
        "author": recipe.get("author", ""),
        "prep_time": recipe.get("prep_time", 0)
    }


def index_recipe(recipe_id, recipe):
    try:
        if es.ping():
            es.index(
                index=INDEX_NAME,
                id=str(recipe_id),
                document=normalize_recipe(recipe),
                refresh=True
            )
    except Exception as e:
        print("Elasticsearch indexing failed:", e)


def make_recipe(title, ingredients, steps, category, author, prep_time):
    return {
        "title": title,
        "ingredients": ingredients,
        "steps": steps,
        "category": category,
        "author": author,
        "prep_time": prep_time,
        "published": True,
        "created_at": time.time()
    }


def ensure_users():
    default_users = [
        {
            "username": "emma",
            "password": "123",
            "favorite_categories": ["Dessert", "Drink"],
            "favorite_ingredients": ["chocolate", "banana", "milk", "berries"]
        },
        {
            "username": "john",
            "password": "123",
            "favorite_categories": ["Main Course", "Starter"],
            "favorite_ingredients": ["chicken", "rice", "cheese", "potato"]
        },
        {
            "username": "sophia",
            "password": "123",
            "favorite_categories": ["Salad", "Dessert"],
            "favorite_ingredients": ["yogurt", "fruit", "tomato", "cucumber"]
        },
        {
            "username": "michael",
            "password": "123",
            "favorite_categories": ["Starter", "Main Course"],
            "favorite_ingredients": ["garlic", "mushroom", "cheese", "egg"]
        },
        {
            "username": "olivia",
            "password": "123",
            "favorite_categories": ["Drink", "Salad"],
            "favorite_ingredients": ["avocado", "berries", "lime", "mint"]
        },
        {
            "username": "daniel",
            "password": "123",
            "favorite_categories": ["Main Course", "Dessert"],
            "favorite_ingredients": ["beef", "burger", "vanilla", "potato"]
        },
        {
            "username": "mia",
            "password": "123",
            "favorite_categories": ["Dessert"],
            "favorite_ingredients": ["apple", "cinnamon", "cream", "chocolate"]
        },
        {
            "username": "noah",
            "password": "123",
            "favorite_categories": ["Main Course"],
            "favorite_ingredients": ["beef", "pasta", "rice", "tomato"]
        },
        {
            "username": "ava",
            "password": "123",
            "favorite_categories": ["Salad", "Drink"],
            "favorite_ingredients": ["cucumber", "lettuce", "orange", "lemon"]
        },
        {
            "username": "liam",
            "password": "123",
            "favorite_categories": ["Starter", "Main Course"],
            "favorite_ingredients": ["bread", "cheese", "chicken", "garlic"]
        },
        {
            "username": "isabella",
            "password": "123",
            "favorite_categories": ["Dessert", "Drink"],
            "favorite_ingredients": ["strawberry", "banana", "honey", "milk"]
        },
        {
            "username": "ethan",
            "password": "123",
            "favorite_categories": ["Main Course"],
            "favorite_ingredients": ["tuna", "pasta", "egg", "rice"]
        },
        {
            "username": "amelia",
            "password": "123",
            "favorite_categories": ["Salad"],
            "favorite_ingredients": ["feta", "olives", "tomato", "avocado"]
        },
        {
            "username": "lucas",
            "password": "123",
            "favorite_categories": ["Starter", "Drink"],
            "favorite_ingredients": ["soup", "lemon", "mint", "bread"]
        },
        {
            "username": "charlotte",
            "password": "123",
            "favorite_categories": ["Dessert"],
            "favorite_ingredients": ["pudding", "vanilla", "berries", "oats"]
        },
        {
            "username": "mason",
            "password": "123",
            "favorite_categories": ["Main Course", "Salad"],
            "favorite_ingredients": ["chicken", "lettuce", "corn", "rice"]
        },
        {
            "username": "harper",
            "password": "123",
            "favorite_categories": ["Drink"],
            "favorite_ingredients": ["smoothie", "orange", "berries", "banana"]
        },
        {
            "username": "logan",
            "password": "123",
            "favorite_categories": ["Main Course", "Starter"],
            "favorite_ingredients": ["burger", "beef", "cheese", "mushroom"]
        },
        {
            "username": "evelyn",
            "password": "123",
            "favorite_categories": ["Salad", "Dessert"],
            "favorite_ingredients": ["apple", "yogurt", "cucumber", "honey"]
        },
        {
            "username": "alex",
            "password": "123",
            "favorite_categories": ["Main Course", "Drink"],
            "favorite_ingredients": ["pizza", "pasta", "lemon", "tea"]
        }
    ]

    for user in default_users:
        users.update_one(
            {"username": user["username"]},
            {
                "$set": {
                    "favorite_categories": user["favorite_categories"],
                    "favorite_ingredients": user["favorite_ingredients"]
                },
                "$setOnInsert": {
                    "username": user["username"],
                    "password": hash_password(user["password"])
                }
            },
            upsert=True
        )

    return default_users


def build_initial_recipes():
    base_recipes = [
        make_recipe(
            "Classic Pancakes",
            [
                "2 cups all-purpose flour",
                "2 tablespoons sugar",
                "1 tablespoon baking powder",
                "1/2 teaspoon salt",
                "2 eggs",
                "1 and 1/2 cups milk",
                "2 tablespoons melted butter",
                "1 teaspoon vanilla extract"
            ],
            """In a large mixing bowl, combine the flour, sugar, baking powder and salt.

In a separate bowl, beat the eggs, then add the milk, melted butter and vanilla extract.

Slowly pour the wet mixture into the dry ingredients while stirring gently.

Mix only until the ingredients are combined. The batter should still have a few small lumps.

Heat a non-stick pan over medium heat and lightly grease it with butter or oil.

Pour a small amount of batter into the pan and form a round pancake.

Cook until small bubbles appear on the surface and the edges look slightly dry.

Turn the pancake over and cook the second side until golden brown.

Repeat the process with the remaining batter.

Serve the pancakes warm with honey, maple syrup, jam, chocolate spread or fresh fruit.""",
            "Dessert",
            "emma",
            25
        ),
        make_recipe(
            "Fresh Lemonade",
            [
                "4 lemons",
                "1 liter cold water",
                "3 tablespoons sugar",
                "Ice cubes",
                "Fresh mint leaves"
            ],
            """Wash the lemons carefully under cold water.

Cut each lemon in half and squeeze the juice into a large jug.

Remove any seeds from the lemon juice.

Add sugar to the jug and stir well until the sugar begins to dissolve.

Pour cold water into the jug and mix everything together.

Taste the lemonade and add more sugar if you prefer a sweeter drink.

Add ice cubes and fresh mint leaves.

Let the lemonade rest for a few minutes so the mint releases its aroma.

Serve cold in glasses with extra lemon slices if desired.""",
            "Drink",
            "emma",
            10
        ),
        make_recipe(
            "Margherita Pizza",
            [
                "1 pizza dough",
                "150 g tomato sauce",
                "200 g mozzarella cheese",
                "Fresh basil",
                "1 tablespoon olive oil",
                "Salt",
                "Oregano"
            ],
            """Preheat the oven to 220°C.

Place the pizza dough on a baking tray lined with baking paper.

Stretch or roll the dough into a round or rectangular shape.

Spread tomato sauce evenly over the dough, leaving a small edge for the crust.

Add mozzarella cheese over the sauce.

Season the pizza with a little salt and oregano.

Drizzle olive oil over the top.

Bake the pizza for 12 to 15 minutes, or until the crust is golden and the cheese is melted.

Remove the pizza from the oven and add fresh basil leaves.

Let it rest for one minute before cutting.

Serve warm.""",
            "Main Course",
            "emma",
            30
        ),
        make_recipe(
            "Creamy Pasta with Cheese",
            [
                "300 g pasta",
                "150 ml cooking cream",
                "100 g grated cheese",
                "1 garlic clove",
                "1 tablespoon butter",
                "Salt",
                "Black pepper"
            ],
            """Bring a large pot of salted water to a boil.

Add the pasta and cook it according to the package instructions.

While the pasta is cooking, melt butter in a pan over medium heat.

Add finely chopped garlic and cook it briefly until fragrant.

Pour in the cooking cream and lower the heat.

Add grated cheese and stir until the cheese melts into the cream.

Season the sauce with salt and black pepper.

Drain the pasta, but keep a small amount of pasta water.

Add the pasta to the sauce and mix well.

If the sauce is too thick, add a little pasta water.

Serve immediately while the pasta is hot and creamy.""",
            "Main Course",
            "emma",
            25
        ),
        make_recipe(
            "Tuna Salad",
            [
                "1 can tuna",
                "Green lettuce",
                "1 tomato",
                "1 cucumber",
                "1 small red onion",
                "2 tablespoons olive oil",
                "1 tablespoon lemon juice",
                "Salt",
                "Black pepper"
            ],
            """Wash the lettuce, tomato and cucumber.

Cut the lettuce into smaller pieces and place it in a large salad bowl.

Slice the tomato and cucumber into bite-sized pieces.

Drain the tuna well and add it to the vegetables.

Slice the red onion thinly and add it to the bowl.

In a small cup, mix olive oil, lemon juice, salt and black pepper.

Pour the dressing over the salad.

Mix gently so the tuna does not break too much.

Serve the salad fresh as a light meal or side dish.""",
            "Salad",
            "emma",
            15
        ),
        make_recipe(
            "Chicken Soup",
            [
                "300 g chicken meat",
                "2 carrots",
                "1 onion",
                "1 potato",
                "1 liter water",
                "Salt",
                "Black pepper",
                "Parsley"
            ],
            """Place the chicken meat in a large pot.

Add water and bring it slowly to a boil.

Peel and chop the carrots, onion and potato.

Add the vegetables to the pot.

Season the soup with salt and black pepper.

Cook over medium heat until the chicken is tender and the vegetables are soft.

Remove the chicken from the pot and cut it into smaller pieces.

Return the chicken pieces to the soup.

Add chopped parsley near the end of cooking.

Taste the soup and adjust the seasoning if needed.

Serve warm as a starter or light meal.""",
            "Starter",
            "john",
            45
        ),
        make_recipe(
            "Grilled Chicken Sandwich",
            [
                "2 slices bread",
                "150 g chicken breast",
                "Lettuce",
                "Tomato",
                "Cheese slice",
                "Mayonnaise",
                "Salt",
                "Black pepper"
            ],
            """Season the chicken breast with salt and black pepper.

Heat a grill pan or regular pan over medium heat.

Cook the chicken until it is golden on the outside and fully cooked inside.

Let the chicken rest for a few minutes, then slice it.

Toast the bread slices lightly.

Spread mayonnaise on one or both slices of bread.

Add lettuce, tomato slices, cheese and grilled chicken.

Close the sandwich and press it gently.

Cut the sandwich in half.

Serve warm with a small salad or fries.""",
            "Main Course",
            "john",
            20
        ),
        make_recipe(
            "Chocolate Muffins",
            [
                "200 g flour",
                "100 g sugar",
                "2 tablespoons cocoa powder",
                "1 teaspoon baking powder",
                "2 eggs",
                "100 ml milk",
                "80 ml oil",
                "Chocolate chips"
            ],
            """Preheat the oven to 180°C.

Prepare a muffin tray with paper muffin cups.

In a large bowl, mix flour, sugar, cocoa powder and baking powder.

In another bowl, whisk together eggs, milk and oil.

Pour the wet ingredients into the dry ingredients.

Mix gently until the batter becomes smooth.

Add chocolate chips and fold them into the batter.

Fill each muffin cup about two thirds full.

Bake for about 20 minutes.

Check with a toothpick if the muffins are done.

Let them cool before serving.""",
            "Dessert",
            "john",
            35
        ),
        make_recipe(
            "Greek Salad",
            [
                "2 tomatoes",
                "1 cucumber",
                "1 red onion",
                "Feta cheese",
                "Olives",
                "Olive oil",
                "Oregano",
                "Salt"
            ],
            """Wash the tomatoes and cucumber.

Cut the tomatoes into larger pieces.

Slice the cucumber into half circles.

Peel and thinly slice the red onion.

Place the vegetables in a salad bowl.

Add olives and pieces of feta cheese.

Sprinkle with oregano and a little salt.

Pour olive oil over the salad.

Mix gently so the feta stays in larger pieces.

Serve fresh as a side dish or light meal.""",
            "Salad",
            "john",
            15
        ),
        make_recipe(
            "Tomato Bruschetta",
            [
                "Baguette slices",
                "2 tomatoes",
                "1 garlic clove",
                "Fresh basil",
                "Olive oil",
                "Salt",
                "Black pepper"
            ],
            """Toast the baguette slices in the oven or in a pan until they become crispy.

Wash the tomatoes and cut them into small cubes.

Place the tomatoes in a bowl.

Add chopped garlic and fresh basil.

Season with salt and black pepper.

Pour in a little olive oil and mix everything together.

Let the tomato mixture sit for a few minutes.

Place the mixture on top of the toasted bread.

Drizzle with a little more olive oil.

Serve immediately so the bread stays crispy.""",
            "Starter",
            "sophia",
            15
        ),
        make_recipe(
            "Spaghetti Bolognese",
            [
                "300 g spaghetti",
                "250 g minced meat",
                "200 ml tomato sauce",
                "1 onion",
                "1 garlic clove",
                "Olive oil",
                "Salt",
                "Black pepper",
                "Oregano"
            ],
            """Cook the spaghetti in salted boiling water until al dente.

Heat olive oil in a pan.

Add chopped onion and cook it until soft.

Add chopped garlic and stir for a short time.

Add minced meat and cook until it changes color.

Pour in the tomato sauce.

Season with salt, black pepper and oregano.

Let the sauce cook slowly for about 15 minutes.

Drain the spaghetti.

Serve the pasta with the sauce on top.

Add grated cheese if desired.""",
            "Main Course",
            "sophia",
            40
        ),
        make_recipe(
            "Fruit Yogurt Bowl",
            [
                "200 g yogurt",
                "1 banana",
                "Strawberries",
                "Blueberries",
                "2 tablespoons oats",
                "1 teaspoon honey"
            ],
            """Place the yogurt into a serving bowl.

Peel and slice the banana.

Wash the strawberries and blueberries.

Cut the strawberries into smaller pieces.

Arrange the fruit on top of the yogurt.

Sprinkle oats over the fruit.

Add honey for sweetness.

Let the bowl rest for a minute so the oats soften slightly.

Serve immediately as breakfast, snack or dessert.""",
            "Dessert",
            "sophia",
            10
        ),
        make_recipe(
            "Orange Smoothie",
            [
                "2 oranges",
                "1 banana",
                "150 ml yogurt",
                "1 teaspoon honey",
                "Ice cubes"
            ],
            """Peel the oranges and remove any seeds.

Peel the banana and cut it into pieces.

Place the orange pieces and banana into a blender.

Add yogurt and honey.

Add a few ice cubes.

Blend until the mixture becomes smooth.

Taste the smoothie and add more honey if needed.

Pour into a glass.

Serve cold immediately.""",
            "Drink",
            "sophia",
            10
        ),
        make_recipe(
            "Caesar Salad",
            [
                "Romaine lettuce",
                "150 g chicken breast",
                "Croutons",
                "Parmesan cheese",
                "Caesar dressing",
                "Salt",
                "Black pepper"
            ],
            """Season the chicken breast with salt and black pepper.

Cook the chicken in a pan until it is fully cooked.

Let the chicken rest for a few minutes and then cut it into strips.

Wash and chop the romaine lettuce.

Place the lettuce in a large bowl.

Add chicken strips, croutons and parmesan cheese.

Pour Caesar dressing over the salad.

Mix gently so the lettuce stays crisp.

Serve fresh as a main salad.""",
            "Salad",
            "sophia",
            25
        ),
        make_recipe(
            "Garlic Bread",
            [
                "1 baguette",
                "80 g butter",
                "2 garlic cloves",
                "Parsley",
                "Salt",
                "Grated cheese"
            ],
            """Preheat the oven to 190°C.

Cut the baguette into slices, but do not cut all the way through.

Soften the butter at room temperature.

Mix the butter with chopped garlic, parsley and salt.

Spread the garlic butter between the bread slices.

Sprinkle grated cheese over the top.

Wrap the bread loosely in baking paper.

Bake for about 10 minutes.

Open the paper and bake for a few more minutes until the bread becomes crispy.

Serve warm as a starter.""",
            "Starter",
            "michael",
            20
        ),
        make_recipe(
            "Vegetable Omelette",
            [
                "3 eggs",
                "1 small bell pepper",
                "1 small onion",
                "50 g cheese",
                "1 tablespoon oil",
                "Salt",
                "Black pepper"
            ],
            """Crack the eggs into a bowl.

Beat the eggs with a fork until the yolks and whites are combined.

Cut the bell pepper and onion into small pieces.

Heat oil in a pan over medium heat.

Add the vegetables and cook them for a few minutes.

Pour the beaten eggs over the vegetables.

Add grated cheese, salt and black pepper.

Cook until the omelette becomes firm.

Fold the omelette in half.

Serve warm with bread or salad.""",
            "Main Course",
            "michael",
            20
        ),
        make_recipe(
            "Apple Cake",
            [
                "3 apples",
                "200 g flour",
                "120 g sugar",
                "2 eggs",
                "100 ml milk",
                "80 ml oil",
                "1 teaspoon baking powder",
                "Cinnamon"
            ],
            """Preheat the oven to 180°C.

Peel the apples and cut them into thin slices.

Mix flour, sugar, baking powder and cinnamon in a bowl.

Add eggs, milk and oil.

Stir until the batter becomes smooth.

Pour the batter into a greased baking dish.

Arrange the apple slices on top.

Sprinkle a little extra cinnamon over the apples.

Bake for about 35 minutes.

Let the cake cool slightly before serving.""",
            "Dessert",
            "michael",
            45
        ),
        make_recipe(
            "Cucumber Yogurt Salad",
            [
                "2 cucumbers",
                "200 g Greek yogurt",
                "1 garlic clove",
                "1 tablespoon olive oil",
                "Salt",
                "Fresh dill"
            ],
            """Wash the cucumbers and cut them into thin slices.

Place the cucumber slices in a bowl.

Chop the garlic very finely.

Mix Greek yogurt with garlic, olive oil and salt.

Pour the yogurt dressing over the cucumbers.

Add fresh dill.

Mix everything gently.

Let the salad cool in the refrigerator for a few minutes.

Serve cold as a refreshing salad.""",
            "Salad",
            "michael",
            15
        ),
        make_recipe(
            "Homemade Hot Chocolate",
            [
                "500 ml milk",
                "100 g dark chocolate",
                "1 tablespoon cocoa powder",
                "1 tablespoon sugar",
                "Whipped cream"
            ],
            """Pour the milk into a small pot.

Warm the milk over low heat, but do not let it boil.

Break the dark chocolate into smaller pieces.

Add the chocolate to the warm milk.

Stir until the chocolate melts completely.

Add cocoa powder and sugar.

Continue stirring until the drink becomes smooth.

Pour into cups.

Add whipped cream on top.

Serve immediately while hot.""",
            "Drink",
            "michael",
            12
        ),
        make_recipe(
            "Caprese Skewers",
            [
                "Cherry tomatoes",
                "Mini mozzarella balls",
                "Fresh basil leaves",
                "Olive oil",
                "Salt",
                "Black pepper"
            ],
            """Wash the cherry tomatoes and basil leaves.

Drain the mini mozzarella balls.

Take small wooden skewers.

Place one tomato, one basil leaf and one mozzarella ball on each skewer.

Repeat until all ingredients are used.

Arrange the skewers on a plate.

Drizzle with olive oil.

Season lightly with salt and black pepper.

Serve cold as a simple starter.""",
            "Starter",
            "olivia",
            10
        ),
        make_recipe(
            "Chicken Rice Bowl",
            [
                "200 g chicken breast",
                "1 cup rice",
                "1 carrot",
                "1 cucumber",
                "Soy sauce",
                "1 tablespoon oil",
                "Salt",
                "Black pepper"
            ],
            """Cook the rice according to the package instructions.

Season the chicken breast with salt and black pepper.

Heat oil in a pan and cook the chicken until golden and fully cooked.

Let the chicken rest and then cut it into strips.

Cut the carrot and cucumber into thin pieces.

Place the rice in a bowl.

Add chicken strips and vegetables on top.

Pour a little soy sauce over the bowl.

Serve warm as a simple main course.""",
            "Main Course",
            "olivia",
            35
        ),
        make_recipe(
            "Banana Oat Cookies",
            [
                "2 ripe bananas",
                "1 cup oats",
                "1 teaspoon cinnamon",
                "Chocolate chips",
                "1 teaspoon honey"
            ],
            """Preheat the oven to 180°C.

Peel the bananas and mash them with a fork.

Add oats and cinnamon to the mashed bananas.

Add honey and chocolate chips.

Mix everything until a sticky dough forms.

Shape small cookies with a spoon.

Place them on a baking tray lined with baking paper.

Bake for about 15 minutes.

Let the cookies cool before serving.""",
            "Dessert",
            "olivia",
            25
        ),
        make_recipe(
            "Berry Smoothie",
            [
                "1 cup mixed berries",
                "1 banana",
                "150 ml milk",
                "100 g yogurt",
                "1 teaspoon honey"
            ],
            """Place the mixed berries into a blender.

Peel the banana and add it to the blender.

Pour in the milk and yogurt.

Add honey for sweetness.

Blend until the smoothie becomes smooth and creamy.

Taste and add more honey if needed.

Pour into a glass.

Serve cold.""",
            "Drink",
            "olivia",
            8
        ),
        make_recipe(
            "Avocado Corn Salad",
            [
                "1 avocado",
                "1 cup corn",
                "1 tomato",
                "1 small red onion",
                "Lime juice",
                "Olive oil",
                "Salt",
                "Black pepper"
            ],
            """Cut the avocado in half and remove the pit.

Scoop out the avocado and cut it into cubes.

Cut the tomato and red onion into small pieces.

Place avocado, corn, tomato and onion in a bowl.

Add lime juice and olive oil.

Season with salt and black pepper.

Mix gently so the avocado keeps its shape.

Serve fresh.""",
            "Salad",
            "olivia",
            15
        ),
        make_recipe(
            "Creamy Mushroom Toast",
            [
                "2 slices bread",
                "200 g mushrooms",
                "1 garlic clove",
                "50 ml cooking cream",
                "1 tablespoon butter",
                "Salt",
                "Black pepper",
                "Parsley"
            ],
            """Clean the mushrooms and slice them.

Melt butter in a pan over medium heat.

Add chopped garlic and cook it briefly.

Add the mushrooms and cook until they become soft.

Pour in the cooking cream.

Season with salt and black pepper.

Let the sauce thicken slightly.

Toast the bread slices.

Place the creamy mushrooms on top of the toast.

Add parsley and serve warm.""",
            "Starter",
            "daniel",
            20
        ),
        make_recipe(
            "Beef Burger",
            [
                "1 burger bun",
                "150 g ground beef",
                "1 cheese slice",
                "Lettuce",
                "Tomato",
                "Pickles",
                "Ketchup",
                "Salt",
                "Black pepper"
            ],
            """Shape the ground beef into a burger patty.

Season both sides with salt and black pepper.

Heat a pan or grill over medium-high heat.

Cook the patty until browned and cooked through.

Add a cheese slice on top and let it melt.

Toast the burger bun lightly.

Spread ketchup on the bun.

Add lettuce, tomato, pickles and the beef patty.

Close the burger and serve warm.""",
            "Main Course",
            "daniel",
            30
        ),
        make_recipe(
            "Vanilla Pudding",
            [
                "500 ml milk",
                "2 tablespoons sugar",
                "1 packet vanilla pudding powder",
                "Fresh fruit",
                "Whipped cream"
            ],
            """Pour most of the milk into a pot and warm it over medium heat.

Mix the pudding powder with the remaining cold milk.

Add sugar to the warm milk.

When the milk is hot, pour in the pudding mixture.

Stir constantly until the pudding thickens.

Remove from heat and pour into small bowls.

Let it cool for a few minutes.

Add fresh fruit and whipped cream before serving.""",
            "Dessert",
            "daniel",
            20
        ),
        make_recipe(
            "Mint Lime Water",
            [
                "1 liter water",
                "1 lime",
                "Fresh mint leaves",
                "Ice cubes",
                "1 teaspoon honey"
            ],
            """Wash the lime and cut it into thin slices.

Place the lime slices in a large jug.

Add fresh mint leaves.

Pour cold water into the jug.

Add honey if you want a slightly sweet drink.

Stir everything together.

Add ice cubes.

Let the drink rest for a few minutes.

Serve cold.""",
            "Drink",
            "daniel",
            8
        ),
        make_recipe(
            "Potato Salad",
            [
                "4 potatoes",
                "1 small onion",
                "2 tablespoons olive oil",
                "1 tablespoon vinegar",
                "Salt",
                "Black pepper",
                "Parsley"
            ],
            """Wash the potatoes and cook them in salted water until soft.

Let the potatoes cool slightly.

Peel the potatoes and cut them into slices.

Slice the onion thinly.

Place potatoes and onion in a large bowl.

Add olive oil, vinegar, salt and black pepper.

Mix gently so the potatoes do not break too much.

Add chopped parsley.

Serve warm or cold.""",
            "Salad",
            "daniel",
            35
        )
    ]

    generated_recipes = build_generated_recipes()

    return base_recipes + generated_recipes


def build_generated_recipes():
    recipe_templates = [
        {
            "category": "Main Course",
            "base_titles": [
                "Chicken Pasta",
                "Beef Rice Bowl",
                "Vegetable Curry",
                "Tuna Pasta",
                "Cheese Pizza",
                "Chicken Wrap",
                "Rice with Vegetables",
                "Creamy Mushroom Pasta",
                "Egg Fried Rice",
                "Turkey Sandwich"
            ],
            "ingredients_pool": [
                "chicken", "beef", "rice", "pasta", "tomato", "cheese",
                "mushroom", "egg", "tuna", "onion", "garlic", "pepper",
                "cream", "olive oil", "carrot", "corn"
            ]
        },
        {
            "category": "Dessert",
            "base_titles": [
                "Chocolate Cake",
                "Banana Muffins",
                "Apple Pie",
                "Vanilla Cream",
                "Strawberry Pancakes",
                "Honey Yogurt Dessert",
                "Oat Cookies",
                "Berry Pudding",
                "Cinnamon Apple Bowl",
                "Chocolate Banana Cup"
            ],
            "ingredients_pool": [
                "chocolate", "banana", "apple", "vanilla", "milk", "sugar",
                "flour", "egg", "honey", "oats", "berries", "yogurt",
                "cream", "cinnamon", "butter"
            ]
        },
        {
            "category": "Salad",
            "base_titles": [
                "Chicken Salad",
                "Greek Vegetable Salad",
                "Avocado Tomato Salad",
                "Corn Cucumber Salad",
                "Tuna Lettuce Salad",
                "Cheese Garden Salad",
                "Potato Onion Salad",
                "Fresh Summer Salad",
                "Tomato Feta Salad",
                "Egg Salad"
            ],
            "ingredients_pool": [
                "lettuce", "tomato", "cucumber", "avocado", "corn",
                "feta", "olives", "chicken", "tuna", "egg", "potato",
                "onion", "olive oil", "lemon", "yogurt"
            ]
        },
        {
            "category": "Drink",
            "base_titles": [
                "Berry Smoothie",
                "Orange Juice",
                "Mint Lemon Water",
                "Banana Milkshake",
                "Iced Tea",
                "Apple Smoothie",
                "Lime Lemonade",
                "Yogurt Fruit Drink",
                "Honey Tea",
                "Cold Chocolate Drink"
            ],
            "ingredients_pool": [
                "berries", "orange", "mint", "lemon", "lime", "banana",
                "milk", "yogurt", "honey", "tea", "water", "ice cubes",
                "apple", "chocolate"
            ]
        },
        {
            "category": "Starter",
            "base_titles": [
                "Garlic Toast",
                "Tomato Bruschetta",
                "Cream Soup",
                "Cheese Bread",
                "Mushroom Toast",
                "Mini Sandwiches",
                "Vegetable Soup",
                "Chicken Bites",
                "Caprese Plate",
                "Warm Potato Starter"
            ],
            "ingredients_pool": [
                "bread", "garlic", "tomato", "cheese", "mushroom",
                "cream", "chicken", "potato", "mozzarella", "basil",
                "onion", "carrot", "olive oil", "butter"
            ]
        }
    ]

    authors = [
        "emma", "john", "sophia", "michael", "olivia", "daniel",
        "mia", "noah", "ava", "liam", "isabella", "ethan",
        "amelia", "lucas", "charlotte", "mason", "harper",
        "logan", "evelyn", "alex"
    ]

    generated = []

    for template in recipe_templates:
        category = template["category"]
        titles = template["base_titles"]
        ingredients_pool = template["ingredients_pool"]

        for title in titles:
            for variant in range(1, 3):
                full_title = f"{title} Variant {variant}"

                ingredients = random.sample(
                    ingredients_pool,
                    k=min(7, len(ingredients_pool))
                )

                prep_time = random.randint(8, 55)
                author = random.choice(authors)

                steps = build_steps(full_title, ingredients, category)

                generated.append(
                    make_recipe(
                        full_title,
                        ingredients,
                        steps,
                        category,
                        author,
                        prep_time
                    )
                )

    random.shuffle(generated)

    return generated[:60]


def build_steps(title, ingredients, category):
    ingredients_text = ", ".join(ingredients)

    if category == "Drink":
        return f"""Prepare all ingredients: {ingredients_text}.

Wash fresh ingredients if needed.

Place the ingredients into a blender or jug.

Add cold water, milk, yogurt or ice depending on the drink.

Mix everything until the drink becomes smooth and fresh.

Taste the drink and add more honey or sugar if needed.

Serve cold in a glass."""

    if category == "Dessert":
        return f"""Prepare all ingredients: {ingredients_text}.

Mix the dry ingredients in one bowl.

Mix the wet ingredients in another bowl.

Combine both mixtures slowly and stir until smooth.

Place the mixture into a baking dish or serving bowl.

Bake, cook or chill the dessert depending on the recipe.

Let it rest for a few minutes before serving."""

    if category == "Salad":
        return f"""Prepare all ingredients: {ingredients_text}.

Wash the vegetables carefully.

Cut the ingredients into smaller pieces.

Place everything into a large salad bowl.

Add olive oil, lemon juice, yogurt dressing or seasoning.

Mix gently so the ingredients keep their shape.

Serve fresh as a light meal or side dish."""

    if category == "Starter":
        return f"""Prepare all ingredients: {ingredients_text}.

Cut the ingredients into smaller pieces.

Heat a pan or oven if the starter is served warm.

Cook or toast the main ingredients for a few minutes.

Season with salt, pepper and herbs.

Arrange everything on a small plate.

Serve before the main course."""

    return f"""Prepare all ingredients: {ingredients_text}.

Cut the vegetables, meat or other main ingredients into smaller pieces.

Heat oil or butter in a pan.

Cook the main ingredients until they become soft and fully cooked.

Add seasoning and mix everything well.

Let the dish cook for a few more minutes.

Serve warm as a main meal."""


def ensure_recipes():
    create_es_index()

    existing_count = recipes.count_documents({"published": True})

    if existing_count >= TARGET_RECIPE_COUNT:
        print(f"Recipes already exist. Current number of recipes: {existing_count}")
        return

    print(f"Current number of recipes: {existing_count}")
    print(f"Creating recipes until there are at least {TARGET_RECIPE_COUNT} recipes...")

    initial_recipes = build_initial_recipes()

    created_count = 0

    existing_titles = set()
    for recipe in recipes.find({"published": True}, {"title": 1}):
        existing_titles.add(recipe.get("title", ""))

    for recipe in initial_recipes:
        if recipes.count_documents({"published": True}) >= TARGET_RECIPE_COUNT:
            break

        if recipe["title"] in existing_titles:
            continue

        result = recipes.insert_one(recipe)
        index_recipe(str(result.inserted_id), recipe)

        existing_titles.add(recipe["title"])
        created_count += 1

    print(f"Created {created_count} new recipes.")
    print(f"Total recipes now: {recipes.count_documents({'published': True})}")


def get_user_profiles():
    return ensure_users()


def get_recipe_documents():
    ensure_recipes()

    all_recipes = list(recipes.find({"published": True}))

    if not all_recipes:
        print("No recipes found.")
        return []

    return all_recipes


def send_event(event):
    producer.send(TOPIC_NAME, event)
    print("Sent:", event)


def recipe_matches_user_profile(recipe, user_profile):
    category = recipe.get("category", "")
    ingredients = [i.lower() for i in recipe.get("ingredients", [])]
    title = recipe.get("title", "").lower()

    favorite_categories = user_profile.get("favorite_categories", [])
    favorite_ingredients = [
        ingredient.lower()
        for ingredient in user_profile.get("favorite_ingredients", [])
    ]

    category_match = category in favorite_categories

    ingredient_match = False

    for favorite in favorite_ingredients:
        if favorite in title:
            ingredient_match = True
            break

        for ingredient in ingredients:
            if favorite in ingredient:
                ingredient_match = True
                break

        if ingredient_match:
            break

    return category_match or ingredient_match


def choose_recipe_for_user(recipe_documents, user_profile):
    matching_recipes = [
        recipe for recipe in recipe_documents
        if recipe_matches_user_profile(recipe, user_profile)
    ]

    if matching_recipes and random.random() < 0.75:
        return random.choice(matching_recipes)

    return random.choice(recipe_documents)


def generate_rating_for_user(recipe, user_profile):
    if recipe_matches_user_profile(recipe, user_profile):
        return random.choice([4, 4, 5, 5, 5])

    return random.choice([1, 2, 2, 3, 3, 4])


def generate_search_query(user_profile):
    favorite_ingredients = user_profile.get("favorite_ingredients", [])
    favorite_categories = user_profile.get("favorite_categories", [])

    general_terms = [
        "chicken",
        "cheese",
        "pasta",
        "chocolate",
        "soup",
        "salad",
        "rice",
        "potato",
        "dessert",
        "quick",
        "pizza",
        "lemon",
        "tuna",
        "banana",
        "smoothie",
        "garlic",
        "tomato",
        "apple",
        "burger",
        "mushroom",
        "avocado",
        "yogurt",
        "vanilla",
        "orange",
        "mint"
    ]

    if favorite_ingredients and random.random() < 0.75:
        query = random.choice(favorite_ingredients)
    else:
        query = random.choice(general_terms)

    if favorite_categories and random.random() < 0.50:
        category = random.choice(favorite_categories)
    else:
        category = random.choice([
            "",
            "Starter",
            "Main Course",
            "Dessert",
            "Drink",
            "Salad"
        ])

    return query, category


def generate_event(user_profiles, recipe_documents):
    event_type = random.choice([
        "recipe_view",
        "recipe_view",
        "recipe_view",
        "recipe_view",
        "recipe_view",
        "recipe_rating",
        "recipe_rating",
        "recipe_search",
        "recipe_search"
    ])

    user_profile = random.choice(user_profiles)
    username = user_profile["username"]

    if event_type == "recipe_view":
        recipe = choose_recipe_for_user(recipe_documents, user_profile)
        recipe_id = str(recipe["_id"])

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "recipe_view",
            "recipe_id": recipe_id,
            "username": username,
            "timestamp": time.time(),
            "extra": {
                "source": "data_generator",
                "device": random.choice(["desktop", "mobile", "tablet"]),
                "session_id": str(uuid.uuid4()),
                "matched_user_profile": recipe_matches_user_profile(recipe, user_profile)
            }
        }

    if event_type == "recipe_rating":
        recipe = choose_recipe_for_user(recipe_documents, user_profile)
        recipe_id = str(recipe["_id"])

        rating = generate_rating_for_user(recipe, user_profile)

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "recipe_rating",
            "recipe_id": recipe_id,
            "username": username,
            "timestamp": time.time(),
            "extra": {
                "rating": rating,
                "source": "data_generator",
                "device": random.choice(["desktop", "mobile", "tablet"]),
                "session_id": str(uuid.uuid4()),
                "matched_user_profile": recipe_matches_user_profile(recipe, user_profile)
            }
        }

    query, category = generate_search_query(user_profile)

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "recipe_search",
        "recipe_id": None,
        "username": username,
        "timestamp": time.time(),
        "extra": {
            "query": query,
            "category": category,
            "source": "data_generator",
            "device": random.choice(["desktop", "mobile", "tablet"]),
            "session_id": str(uuid.uuid4())
        }
    }


def main():
    user_profiles = get_user_profiles()
    recipe_documents = get_recipe_documents()

    if not recipe_documents:
        return

    print("Data generator started.")
    print("Number of users:", len(user_profiles))
    print("Number of recipes:", len(recipe_documents))
    print("Number of events to generate:", NUMBER_OF_EVENTS)

    for _ in range(NUMBER_OF_EVENTS):
        event = generate_event(user_profiles, recipe_documents)
        send_event(event)
        time.sleep(0.01)

    producer.flush()

    print("Data generation finished.")
    print("Generated events:", NUMBER_OF_EVENTS)
    print("Users:", len(user_profiles))
    print("Recipes:", recipes.count_documents({"published": True}))


if __name__ == "__main__":
    main()