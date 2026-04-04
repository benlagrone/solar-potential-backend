ALL_BANDS = ["cold", "middle", "warm"]


def fit(rating, action, zone_bands=None):
    return {
        "rating": rating,
        "action": action,
        "zoneBands": zone_bands or ALL_BANDS,
    }


PLANTING_DATA_SOURCES = [
    {
        "id": "usda-hardiness-map",
        "label": "USDA Plant Hardiness Zone Map",
        "url": "https://planthardiness.ars.usda.gov/",
    },
    {
        "id": "vt-vegetable-guide",
        "label": "Virginia Tech Home Garden Vegetable Planting Guide",
        "url": "https://www.pubs.ext.vt.edu/426/426-331/426-331.htmll.html",
    },
    {
        "id": "umn-planting-garden",
        "label": "University of Minnesota Extension: Planting the vegetable garden",
        "url": "https://extension.umn.edu/node/8861",
    },
    {
        "id": "umn-cool-season",
        "label": "University of Minnesota Extension: Growing cool-season vegetables in Minnesota",
        "url": "https://extension.umn.edu/yard-and-garden-news/growing-cool-season-vegetables-minnesota",
    },
    {
        "id": "psu-sun-shade",
        "label": "Penn State Extension: Planting in Sun or Shade",
        "url": "https://extension.psu.edu/planting-in-sun-or-shade",
    },
    {
        "id": "uga-siting-garden",
        "label": "UGA Cooperative Extension: Siting a Garden",
        "url": "https://extension.uga.edu/publications/detail.html?number=C1027-2",
    },
    {
        "id": "psu-asparagus",
        "label": "Penn State Extension: Growing Asparagus in the Home Garden",
        "url": "https://extension.psu.edu/growing-asparagus-in-the-home-garden/",
    },
    {
        "id": "umn-rhubarb",
        "label": "University of Minnesota Extension: Growing rhubarb in home gardens",
        "url": "https://extension.umn.edu/vegetables/growing-rhubarb",
    },
    {
        "id": "umn-strawberry",
        "label": "University of Minnesota Extension: Growing strawberries in the home garden",
        "url": "https://extension.umn.edu/fruit/growing-strawberries-home-garden",
    },
    {
        "id": "umn-blueberry",
        "label": "University of Minnesota Extension: Growing blueberries in the home garden",
        "url": "https://extension.umn.edu/node/416",
    },
    {
        "id": "umd-rosemary",
        "label": "University of Maryland Extension: Rosemary",
        "url": "https://extension.umd.edu/resource/rosemary/",
    },
    {
        "id": "umd-lavender",
        "label": "University of Maryland Extension: Lavender",
        "url": "https://extension.umd.edu/resource/lavender",
    },
    {
        "id": "umd-chives",
        "label": "University of Maryland Extension: Chives",
        "url": "https://extension.umd.edu/resource/chives/",
    },
    {
        "id": "umd-sage",
        "label": "University of Maryland Extension: Sage",
        "url": "https://extension.umd.edu/resource/sage",
    },
    {
        "id": "umn-herbs",
        "label": "University of Minnesota Extension: Growing herbs in home gardens",
        "url": "https://extension.umn.edu/vegetables/growing-herbs",
    },
]


CROP_CATALOG = [
    {
        "id": "tomato",
        "name": "Tomatoes",
        "category": "fruiting",
        "lifecycle": "annual",
        "heatProfile": "warm-season",
        "sun": {
            "primary": ["full-sun"],
            "secondary": ["part-sun"],
        },
        "seasonalFit": {
            "spring": fit(3, "Set transplants out after frost and warm the brightest bed first."),
            "summer": fit(3, "Main production crop for the highest-light stretch of the season."),
            "fall": fit(1, "Late plantings only make sense in long, mild seasons.", ["warm"]),
        },
        "sourceIds": ["umn-planting-garden", "vt-vegetable-guide", "uga-siting-garden"],
    },
    {
        "id": "pepper",
        "name": "Peppers",
        "category": "fruiting",
        "lifecycle": "annual",
        "heatProfile": "warm-season",
        "sun": {
            "primary": ["full-sun"],
            "secondary": ["part-sun"],
        },
        "seasonalFit": {
            "spring": fit(3, "Transplant once nights stay reliably warm and frost risk is gone."),
            "summer": fit(3, "Strong main-season crop in bright, heat-holding beds."),
            "fall": fit(
                1,
                "Second rounds favor warm zones with a long late-season window.",
                ["warm"],
            ),
        },
        "sourceIds": ["umn-planting-garden", "vt-vegetable-guide", "uga-siting-garden"],
    },
    {
        "id": "eggplant",
        "name": "Eggplant",
        "category": "fruiting",
        "lifecycle": "annual",
        "heatProfile": "long-season",
        "sun": {
            "primary": ["full-sun"],
            "secondary": ["part-sun"],
        },
        "seasonalFit": {
            "spring": fit(2, "Best started from transplants after the soil is warm."),
            "summer": fit(3, "Hot, bright weather is the real payoff window for this crop."),
        },
        "sourceIds": ["umn-planting-garden", "uga-siting-garden"],
    },
    {
        "id": "cucumber",
        "name": "Cucumbers",
        "category": "fruiting",
        "lifecycle": "annual",
        "heatProfile": "warm-season",
        "sun": {
            "primary": ["full-sun"],
            "secondary": ["part-sun"],
        },
        "seasonalFit": {
            "spring": fit(3, "Direct sow or transplant once frost danger has passed."),
            "summer": fit(3, "Fast main-season crop for trellises or open warm beds."),
            "fall": fit(
                1,
                "Late sowings work best where fall stays warm longer.",
                ["middle", "warm"],
            ),
        },
        "sourceIds": ["umn-planting-garden", "vt-vegetable-guide", "uga-siting-garden"],
    },
    {
        "id": "summer-squash",
        "name": "Summer squash",
        "category": "fruiting",
        "lifecycle": "annual",
        "heatProfile": "warm-season",
        "sun": {
            "primary": ["full-sun"],
            "secondary": ["part-sun"],
        },
        "seasonalFit": {
            "spring": fit(2, "Seed or transplant after frost into roomy, bright space."),
            "summer": fit(3, "Heavy summer producer when the bed gets strong direct light."),
            "fall": fit(1, "Late rounds are mostly for longer warm-zone seasons.", ["warm"]),
        },
        "sourceIds": ["umn-planting-garden", "vt-vegetable-guide", "uga-siting-garden"],
    },
    {
        "id": "bush-bean",
        "name": "Bush beans",
        "category": "legume",
        "lifecycle": "annual",
        "heatProfile": "warm-season",
        "sun": {
            "primary": ["full-sun"],
            "secondary": ["part-sun"],
        },
        "seasonalFit": {
            "spring": fit(2, "Wait for warm soil, then direct sow in quick succession blocks."),
            "summer": fit(3, "Reliable main-season crop with repeat sowings."),
            "fall": fit(
                2,
                "Late sowings stay workable if the fall window is still warm.",
                ["middle", "warm"],
            ),
        },
        "sourceIds": ["umn-planting-garden", "vt-vegetable-guide"],
    },
    {
        "id": "pole-bean",
        "name": "Pole beans",
        "category": "legume",
        "lifecycle": "annual",
        "heatProfile": "warm-season",
        "sun": {
            "primary": ["full-sun"],
            "secondary": ["part-sun"],
        },
        "seasonalFit": {
            "spring": fit(2, "Direct sow after frost once supports are in place."),
            "summer": fit(3, "Strong choice for bright vertical production."),
            "fall": fit(
                1,
                "Late sowings favor mild zones that hold heat deeper into fall.",
                ["warm"],
            ),
        },
        "sourceIds": ["umn-planting-garden", "vt-vegetable-guide"],
    },
    {
        "id": "sweet-corn",
        "name": "Sweet corn",
        "category": "grain",
        "lifecycle": "annual",
        "heatProfile": "warm-season",
        "sun": {
            "primary": ["full-sun"],
            "secondary": [],
        },
        "seasonalFit": {
            "spring": fit(
                2,
                "Plant once the soil has warmed and you can give it a block, not a row.",
            ),
            "summer": fit(3, "Needs the brightest, longest-light section of the garden."),
        },
        "sourceIds": ["umn-planting-garden", "uga-siting-garden"],
    },
    {
        "id": "okra",
        "name": "Okra",
        "category": "fruiting",
        "lifecycle": "annual",
        "heatProfile": "hot-season",
        "sun": {
            "primary": ["full-sun"],
            "secondary": [],
        },
        "seasonalFit": {
            "spring": fit(
                1,
                "Wait for real heat before planting or growth will stall.",
                ["middle", "warm"],
            ),
            "summer": fit(
                3,
                "One of the best high-heat crops for bright summer beds.",
                ["middle", "warm"],
            ),
            "fall": fit(2, "Can keep producing late where autumn stays warm.", ["warm"]),
        },
        "sourceIds": ["vt-vegetable-guide", "uga-siting-garden"],
    },
    {
        "id": "melon",
        "name": "Melons",
        "category": "fruiting",
        "lifecycle": "annual",
        "heatProfile": "long-season",
        "sun": {
            "primary": ["full-sun"],
            "secondary": [],
        },
        "seasonalFit": {
            "spring": fit(
                1,
                "Use only the hottest, most protected bed and favor short-season varieties.",
            ),
            "summer": fit(3, "Best where the brightest zone also holds heat well."),
        },
        "sourceIds": ["umn-planting-garden", "uga-siting-garden"],
    },
    {
        "id": "basil",
        "name": "Basil",
        "category": "herb",
        "lifecycle": "annual",
        "heatProfile": "warm-season",
        "sun": {
            "primary": ["full-sun", "part-sun"],
            "secondary": [],
        },
        "seasonalFit": {
            "spring": fit(2, "Plant after frost as the warm-season bed comes online."),
            "summer": fit(3, "Excellent companion for tomatoes, peppers, and summer containers."),
            "fall": fit(
                1,
                "Late plantings are worthwhile mainly in mild, warm zones.",
                ["warm"],
            ),
        },
        "sourceIds": ["umn-planting-garden", "vt-vegetable-guide"],
    },
    {
        "id": "lettuce",
        "name": "Lettuce",
        "category": "greens",
        "lifecycle": "annual",
        "heatProfile": "cool-season",
        "sun": {
            "primary": ["part-sun", "part-shade"],
            "secondary": ["full-sun"],
        },
        "seasonalFit": {
            "spring": fit(3, "Direct sow or transplant early while conditions are still cool."),
            "summer": fit(
                1,
                "Works best with gentler light, steady moisture, and heat-tolerant varieties.",
                ["cold", "middle"],
            ),
            "fall": fit(3, "Top-tier succession crop once summer heat breaks."),
            "winter": fit(1, "Possible with protection in mild zones.", ["warm"]),
        },
        "sourceIds": ["umn-cool-season", "umn-planting-garden", "psu-sun-shade"],
    },
    {
        "id": "spinach",
        "name": "Spinach",
        "category": "greens",
        "lifecycle": "annual",
        "heatProfile": "cool-season",
        "sun": {
            "primary": ["part-sun", "part-shade"],
            "secondary": ["full-sun"],
        },
        "seasonalFit": {
            "spring": fit(3, "One of the earliest crops to push into cool soil."),
            "summer": fit(
                1,
                "Only realistic in cooler zones or gentler light before bolting.",
                ["cold"],
            ),
            "fall": fit(3, "Excellent fall return crop as nights cool off."),
            "winter": fit(1, "Mild-zone winter production is possible with cover.", ["warm"]),
        },
        "sourceIds": ["umn-cool-season", "umn-planting-garden", "psu-sun-shade"],
    },
    {
        "id": "kale",
        "name": "Kale",
        "category": "greens",
        "lifecycle": "annual",
        "heatProfile": "cool-season",
        "sun": {
            "primary": ["full-sun", "part-sun"],
            "secondary": ["part-shade"],
        },
        "seasonalFit": {
            "spring": fit(3, "Strong spring brassica for beds that warm up early."),
            "summer": fit(1, "Holds better than lettuce, especially with afternoon relief."),
            "fall": fit(3, "One of the best cool-season fall crops for repeat harvests."),
            "winter": fit(2, "Can carry deep into mild winters with some protection.", ["warm"]),
        },
        "sourceIds": ["umn-cool-season", "umn-planting-garden", "vt-vegetable-guide"],
    },
    {
        "id": "broccoli",
        "name": "Broccoli",
        "category": "brassica",
        "lifecycle": "annual",
        "heatProfile": "cool-season",
        "sun": {
            "primary": ["full-sun", "part-sun"],
            "secondary": [],
        },
        "seasonalFit": {
            "spring": fit(3, "Use the early cool window before real heat arrives."),
            "fall": fit(3, "Often performs better in fall than spring when heat stress is lower."),
            "winter": fit(1, "Only with protection in mild zones.", ["warm"]),
        },
        "sourceIds": ["umn-cool-season", "umn-planting-garden", "vt-vegetable-guide"],
    },
    {
        "id": "cabbage",
        "name": "Cabbage",
        "category": "brassica",
        "lifecycle": "annual",
        "heatProfile": "cool-season",
        "sun": {
            "primary": ["full-sun", "part-sun"],
            "secondary": [],
        },
        "seasonalFit": {
            "spring": fit(3, "Transplant into the early shoulder season for firm heads."),
            "fall": fit(3, "Very strong fall crop once heat moderates."),
            "winter": fit(1, "Mild-zone growing is possible with cover.", ["warm"]),
        },
        "sourceIds": ["umn-cool-season", "umn-planting-garden", "vt-vegetable-guide"],
    },
    {
        "id": "carrot",
        "name": "Carrots",
        "category": "root",
        "lifecycle": "annual",
        "heatProfile": "cool-season",
        "sun": {
            "primary": ["full-sun", "part-sun"],
            "secondary": ["part-shade"],
        },
        "seasonalFit": {
            "spring": fit(3, "Direct sow early for a long root-development window."),
            "summer": fit(
                1,
                "Short successions are still workable with moisture and lighter heat.",
                ["cold", "middle"],
            ),
            "fall": fit(3, "Excellent late-summer sowing for fall harvest."),
            "winter": fit(1, "Can overwinter in mild zones with mulch or cover.", ["warm"]),
        },
        "sourceIds": ["umn-planting-garden", "vt-vegetable-guide", "psu-sun-shade"],
    },
    {
        "id": "beet",
        "name": "Beets",
        "category": "root",
        "lifecycle": "annual",
        "heatProfile": "cool-season",
        "sun": {
            "primary": ["full-sun", "part-sun"],
            "secondary": ["part-shade"],
        },
        "seasonalFit": {
            "spring": fit(3, "Easy direct-sow shoulder-season root crop."),
            "summer": fit(
                1,
                "Works in lighter heat, especially where afternoons are gentler.",
                ["cold", "middle"],
            ),
            "fall": fit(3, "Very strong fall sowing candidate."),
            "winter": fit(1, "Possible with protection in mild zones.", ["warm"]),
        },
        "sourceIds": ["umn-planting-garden", "vt-vegetable-guide", "psu-sun-shade"],
    },
    {
        "id": "radish",
        "name": "Radishes",
        "category": "root",
        "lifecycle": "annual",
        "heatProfile": "cool-season",
        "sun": {
            "primary": ["full-sun", "part-sun", "part-shade"],
            "secondary": [],
        },
        "seasonalFit": {
            "spring": fit(3, "Fast shoulder-season filler between slower crops."),
            "summer": fit(
                1,
                "Best in cooler zones or with some midday relief.",
                ["cold", "middle"],
            ),
            "fall": fit(3, "Excellent quick crop for late-summer resets."),
            "winter": fit(1, "Mild winters can support short rounds under cover.", ["warm"]),
        },
        "sourceIds": ["umn-planting-garden", "umn-cool-season", "psu-sun-shade"],
    },
    {
        "id": "peas",
        "name": "Peas",
        "category": "legume",
        "lifecycle": "annual",
        "heatProfile": "cool-season",
        "sun": {
            "primary": ["full-sun", "part-sun"],
            "secondary": [],
        },
        "seasonalFit": {
            "spring": fit(3, "Classic early-season crop for the cool shoulder."),
            "fall": fit(
                2,
                "Late-summer sowings are worth trying where early frosts do not slam shut.",
                ["middle", "warm"],
            ),
        },
        "sourceIds": ["umn-planting-garden", "vt-vegetable-guide"],
    },
    {
        "id": "cilantro",
        "name": "Cilantro",
        "category": "herb",
        "lifecycle": "annual",
        "heatProfile": "cool-season",
        "sun": {
            "primary": ["part-sun", "part-shade"],
            "secondary": ["full-sun"],
        },
        "seasonalFit": {
            "spring": fit(3, "Best used as an early cool-season herb."),
            "summer": fit(
                1,
                "Only really workable with gentler light or cooler conditions.",
                ["cold"],
            ),
            "fall": fit(3, "Excellent return crop once temperatures soften."),
            "winter": fit(1, "Possible in mild zones with cover.", ["warm"]),
        },
        "sourceIds": ["umn-cool-season", "psu-sun-shade"],
    },
    {
        "id": "parsley",
        "name": "Parsley",
        "category": "herb",
        "lifecycle": "biennial",
        "heatProfile": "flex-season",
        "sun": {
            "primary": ["full-sun", "part-sun"],
            "secondary": ["part-shade"],
        },
        "seasonalFit": {
            "spring": fit(2, "Slow starter, but dependable once established."),
            "summer": fit(2, "Good bridge herb for mixed beds and containers."),
            "fall": fit(2, "Can keep going late into the season."),
            "winter": fit(1, "Mild-zone production is possible with some cover.", ["warm"]),
        },
        "sourceIds": ["psu-sun-shade", "umn-herbs"],
    },
    {
        "id": "chives",
        "name": "Chives",
        "category": "herb",
        "lifecycle": "perennial",
        "heatProfile": "flex-season",
        "perennialRange": {
            "min": 3,
            "max": 9.5,
        },
        "sun": {
            "primary": ["full-sun", "part-sun"],
            "secondary": ["part-shade"],
        },
        "seasonalFit": {
            "spring": fit(3, "Plant or divide in spring for a long-term perennial edging herb."),
            "summer": fit(2, "Steady cut-and-come-again herb through the main season."),
            "fall": fit(3, "Good time to plant or divide where roots can settle in."),
        },
        "sourceIds": ["umd-chives", "umn-herbs"],
    },
    {
        "id": "mint",
        "name": "Mint",
        "category": "herb",
        "lifecycle": "perennial",
        "heatProfile": "flex-season",
        "perennialRange": {
            "min": 4,
            "max": 9.5,
        },
        "sun": {
            "primary": ["part-sun", "part-shade"],
            "secondary": ["full-sun", "shade"],
        },
        "seasonalFit": {
            "spring": fit(
                3,
                "Reliable perennial for edges or containers, especially in gentler light.",
            ),
            "summer": fit(2, "Handles partial shade well during hotter stretches."),
            "fall": fit(2, "Easy perennial to establish before winter in suitable zones."),
        },
        "sourceIds": ["umn-herbs", "psu-sun-shade"],
    },
    {
        "id": "thyme",
        "name": "Thyme",
        "category": "herb",
        "lifecycle": "perennial",
        "heatProfile": "flex-season",
        "perennialRange": {
            "min": 5,
            "max": 9,
        },
        "sun": {
            "primary": ["full-sun", "part-sun"],
            "secondary": [],
        },
        "seasonalFit": {
            "spring": fit(3, "Strong low-growing perennial herb for sunny edges."),
            "summer": fit(2, "Performs well in lean, bright, drier beds."),
            "fall": fit(2, "Can be planted in fall where winters are not extreme."),
        },
        "sourceIds": ["umn-herbs", "psu-sun-shade"],
    },
    {
        "id": "sage",
        "name": "Sage",
        "category": "herb",
        "lifecycle": "perennial",
        "heatProfile": "flex-season",
        "perennialRange": {
            "min": 5,
            "max": 8.5,
        },
        "sun": {
            "primary": ["full-sun"],
            "secondary": ["part-sun"],
        },
        "seasonalFit": {
            "spring": fit(
                3,
                "Best planted where the soil drains well and the bed stays bright.",
            ),
            "summer": fit(2, "Useful perennial herb in dry, sunny beds."),
            "fall": fit(
                2,
                "Fall planting is fine in milder zones with good drainage.",
                ["middle", "warm"],
            ),
        },
        "sourceIds": ["umd-sage", "umn-herbs"],
    },
    {
        "id": "rosemary",
        "name": "Rosemary",
        "category": "herb",
        "lifecycle": "perennial",
        "heatProfile": "warm-season",
        "perennialRange": {
            "min": 8,
            "max": 10.5,
            "containerFriendlyBelowMin": True,
        },
        "sun": {
            "primary": ["full-sun"],
            "secondary": ["part-sun"],
        },
        "seasonalFit": {
            "spring": fit(2, "Plant in spring if you want time to size it up before winter."),
            "summer": fit(2, "Thrives in hot, sunny spots with excellent drainage."),
            "fall": fit(2, "Good outdoor perennial only in mild-winter zones.", ["warm"]),
        },
        "sourceIds": ["umd-rosemary", "umn-herbs"],
    },
    {
        "id": "lavender",
        "name": "Lavender",
        "category": "herb",
        "lifecycle": "perennial",
        "heatProfile": "warm-season",
        "perennialRange": {
            "min": 5,
            "max": 8.5,
        },
        "sun": {
            "primary": ["full-sun"],
            "secondary": [],
        },
        "seasonalFit": {
            "spring": fit(2, "Plant into lean, sharply drained sunny soil."),
            "summer": fit(2, "Best in the driest, brightest spot available."),
            "fall": fit(
                1,
                "Fall planting needs mild winters and very good drainage.",
                ["middle", "warm"],
            ),
        },
        "sourceIds": ["umd-lavender", "umn-herbs"],
    },
    {
        "id": "asparagus",
        "name": "Asparagus",
        "category": "perennial-vegetable",
        "lifecycle": "perennial",
        "heatProfile": "flex-season",
        "perennialRange": {
            "min": 3,
            "max": 8.5,
        },
        "sun": {
            "primary": ["full-sun"],
            "secondary": ["part-sun"],
        },
        "seasonalFit": {
            "spring": fit(3, "Spring is the main planting window for crowns and bed establishment."),
            "fall": fit(1, "Fall planting is less common and less forgiving.", ["warm"]),
        },
        "sourceIds": ["psu-asparagus", "vt-vegetable-guide"],
    },
    {
        "id": "rhubarb",
        "name": "Rhubarb",
        "category": "perennial-vegetable",
        "lifecycle": "perennial",
        "heatProfile": "cool-season",
        "perennialRange": {
            "min": 3,
            "max": 7.5,
        },
        "sun": {
            "primary": ["full-sun", "part-sun"],
            "secondary": [],
        },
        "seasonalFit": {
            "spring": fit(3, "Classic hardy perennial for colder-zone gardens."),
            "fall": fit(2, "Division and planting fit best in cooler climates.", ["cold", "middle"]),
        },
        "sourceIds": ["umn-rhubarb"],
    },
    {
        "id": "strawberry",
        "name": "Strawberries",
        "category": "berry",
        "lifecycle": "perennial",
        "heatProfile": "flex-season",
        "perennialRange": {
            "min": 4,
            "max": 8.5,
        },
        "sun": {
            "primary": ["full-sun"],
            "secondary": ["part-sun"],
        },
        "seasonalFit": {
            "spring": fit(3, "Strong spring planting choice for a long-lived patch."),
            "summer": fit(2, "Established plants hold their place through the main season."),
            "fall": fit(3, "Good time to reset or expand a patch in suitable climates."),
        },
        "sourceIds": ["umn-strawberry", "psu-sun-shade"],
    },
    {
        "id": "blueberry",
        "name": "Blueberries",
        "category": "berry",
        "lifecycle": "perennial",
        "heatProfile": "flex-season",
        "perennialRange": {
            "min": 3,
            "max": 8.5,
        },
        "sun": {
            "primary": ["full-sun"],
            "secondary": ["part-sun"],
        },
        "seasonalFit": {
            "spring": fit(3, "Best planted in spring once acidic soil prep is handled."),
            "fall": fit(
                3,
                "Fall planting also works where roots can settle before hard freezes.",
            ),
        },
        "sourceIds": ["umn-blueberry", "psu-sun-shade"],
    },
    {
        "id": "fig",
        "name": "Figs",
        "category": "fruit",
        "lifecycle": "perennial",
        "heatProfile": "warm-season",
        "perennialRange": {
            "min": 7,
            "max": 10.5,
            "containerFriendlyBelowMin": True,
        },
        "sun": {
            "primary": ["full-sun"],
            "secondary": [],
        },
        "seasonalFit": {
            "spring": fit(
                2,
                "Plant where winter cold will not knock it back every year.",
                ["middle", "warm"],
            ),
            "summer": fit(
                2,
                "Best in hot, reflective, protected beds or patio zones.",
                ["middle", "warm"],
            ),
            "fall": fit(1, "Outdoor establishment favors mild winters.", ["warm"]),
        },
        "sourceIds": ["usda-hardiness-map", "uga-siting-garden"],
    },
    {
        "id": "artichoke",
        "name": "Artichokes",
        "category": "perennial-vegetable",
        "lifecycle": "perennial",
        "heatProfile": "warm-season",
        "perennialRange": {
            "min": 7,
            "max": 10,
            "containerFriendlyBelowMin": False,
        },
        "sun": {
            "primary": ["full-sun"],
            "secondary": ["part-sun"],
        },
        "seasonalFit": {
            "spring": fit(
                2,
                "Worth trying where the growing season is long or winters are mild.",
                ["middle", "warm"],
            ),
            "summer": fit(2, "Needs space and strong light to justify the footprint."),
            "fall": fit(1, "Outdoor fall planting is mainly for mild zones.", ["warm"]),
        },
        "sourceIds": ["usda-hardiness-map", "uga-siting-garden"],
    },
]


GARDEN_CROP_CATALOG_SEED = {
    "catalog_id": "default",
    "version": "2026-04-03",
    "model_note": (
        "USDA hardiness is useful for crop type and perennial survival, but exact sowing and "
        "transplant dates still require local frost and soil-temperature calibration."
    ),
    "source_basis": PLANTING_DATA_SOURCES,
    "crops": CROP_CATALOG,
}
