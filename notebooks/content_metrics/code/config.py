"""Module containing basic configuration information for the dashboard queries.
Changes to the way subjects are organized (e.g. what counts as middle school
math, adding a new core academic subject, etc.) can be updated here. """

import seaborn as sns

# This will be used to calculate stats for each node in the dict
# Each node corresponds to a domain, subject or list of subjects
COMPARISON_NODES = {
    "EE": {"subject": "electrical-engineering"},
    "Biology": {"subject": "biology"},
    "Chemistry": {"subject_list": ["chemistry"]},
    "Physics": {"subject": "physics"},
    "ES Math": {"subject_list": [
            'cc-sixth-grade-math',
            'cc-fifth-grade-math',
            'cc-fourth-grade-math',
            'cc-third-grade-math',
            'early-math',
            'arithmetic',
            'basic-geo',
            'pre-algebra',
            'fr-sixth-grade-math',
            'fr-fifth-grade-math',
            'fr-first-grade-math',
            'fr-fourth-grade-math',
            'fr-third-grade-math',
            'fr-second-grade-math',
            'ab-sixth-grade-math',
            'on-sixth-grade-math',
            'in-fifth-grade-math',
            'in-sixth-grade-math']},
    "MS Math": {"subject_list": [
            'cc-eighth-grade-math',
            'cc-seventh-grade-math',
            'in-seventh-grade-math'
            'in-eighth-grade-math',
            'fr-eleventh-grade-math',
            'fr-tenth-grade-math',
            'fr-eigth-grade-math',
            'fr-seventh-grade-math']},
    "HS Math": {"subject_list": [
            'differential-calculus',
            'differential-equations',
            'geometry',
            'integral-calculus',
            'linear-algebra',
            'multivariable-calculus',
            'algebra',
            'algebra-basics',
            'algebra2',
            'precalculus',
            'probability',
            'recreational-math',
            'trigonometry',
            'enem']},
    "Art History": {"subject_list": [
            'art-history-basics',
            'renaissance-reformation',
            'global-culture',
            'prehistoric-art',
            'monarchy-enlightenment',
            'art-asia',
            'ancient-art-civilizations',
            'becoming-modern',
            'art-africa',
            'medieval-world',
            'becoming-modern',
            'art-oceania',
            'art-islam',
            'art-1010',
            'art-history-for-teachers']},
    "History": {"subject": "history"},
    "Computer Programming": {"subject": "computer-programming"},
    "Computer Science": {"subject": "computer-science"},
    "Economics & Finance": {"domain": "economics-finance-domain"},
    "College Admissions": {"subject": "college-admissions"},
    "Health and Medicine": {"subject": "health-and-medicine"},
    "Pixar": {"subject": "pixar"}
}

# This will be used to calculate stats in aggregate across all subjects listed
SUMMARY_NODE = {"Core Academic": {"subject_list":
                                  ["physics",
                                   "chemistry",
                                   "biology",
                                   "electrical-engineering",
                                   "organic-chemistry",
                                   "computer-programming",
                                   "art-history-basics",
                                   "renaissance-reformation",
                                   "global-culture",
                                   "prehistoric-art",
                                   "monarchy-enlightenment",
                                   "art-asia",
                                   "ancient-art-civilizations",
                                   "becoming-modern",
                                   "art-africa",
                                   "medieval-world",
                                   "becoming-modern",
                                   "art-oceania",
                                   "art-islam",
                                   "art-1010",
                                   "art-history-for-teachers"
                                   "history",
                                   "microeconomics",
                                   "macroeconomics",
                                   "core-finance",
                                   "entrepreneurship2",
                                   'cc-sixth-grade-math',
                                   'cc-fifth-grade-math',
                                   'cc-fourth-grade-math',
                                   'cc-third-grade-math',
                                   'early-math',
                                   'arithmetic',
                                   'basic-geo',
                                   'pre-algebra',
                                   'fr-sixth-grade-math',
                                   'fr-fifth-grade-math',
                                   'fr-first-grade-math',
                                   'fr-fourth-grade-math',
                                   'fr-third-grade-math',
                                   'fr-second-grade-math',
                                   'ab-sixth-grade-math',
                                   'on-sixth-grade-math',
                                   'in-fifth-grade-math',
                                   'in-sixth-grade-math',
                                   'cc-eighth-grade-math',
                                   'cc-seventh-grade-math',
                                   'in-seventh-grade-math'
                                   'in-eighth-grade-math',
                                   'fr-eleventh-grade-math',
                                   'fr-tenth-grade-math',
                                   'fr-eigth-grade-math',
                                   'fr-seventh-grade-math',
                                   'differential-calculus',
                                   'differential-equations',
                                   'geometry',
                                   'integral-calculus',
                                   'linear-algebra',
                                   'multivariable-calculus',
                                   'algebra',
                                   'algebra-basics',
                                   'algebra2',
                                   'precalculus',
                                   'probability',
                                   'recreational-math',
                                   'trigonometry',
                                   'enem']}
}
NODES = [
 'Core Academic',
 'HS Math',
 'MS Math',
 'ES Math',
 'Chemistry',
 'Physics',
 'Biology',
 'Health and Medicine',
 'EE',
 'Economics & Finance',
 'College Admissions',
 'History',
 'Art History',
 'Computer Programming',
 'Computer Science',
 'Pixar']

# Create color palette for this configuration
COLORS = (sns.color_palette("PuBuGn_r", 4)  # Math
          + sns.color_palette("afmhot", 5)  # Science
          + sns.color_palette("YlGn", 4)  # Misc
          + sns.color_palette("cool", 3)  # Computing & Pixar
          )
COLOR_MAP = {NODES[i]: COLORS[i] for i in range(len(NODES))}


P_TYPES = ["video", "exercise", "article", "tutorial", "project", "challenge"]
C_TYPES = ["video", "exercise", "article", "talkthrough", "scratchpad"]
