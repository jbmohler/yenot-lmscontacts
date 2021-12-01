import os
import random
import yenot.backend.api as api
from . import word_list

app = api.get_global_app()

alpha = "abcdefghijklmnopqrstuvwxyz"
vowels = "aeiou"
consonants = "".join([c for c in alpha if c not in vowels])
vowels += "y"
numbers = "0123456789"
symbols = "`~!@#$%^&*()[]{}:;/.,<>?"

digraphs = ["th", "sh", "ch", "st", "kn", "wh"]


def triplet():
    """
    Generate a short 'pronounciable' bit.
    """
    ends = digraphs + list(consonants)
    spaces = [ends, list(vowels), ends]
    bit = "".join([random.sample(x, 1)[0] for x in spaces])
    if random.randint(0, 1) == 1:
        bit = bit.title()
    return bit


def pronounciable(bits, tipSpace=None):
    """
    :param minlen:  minimum password length
    :param maxlen:  maximum password length

    >>> 0 <= len(pronounciable(0, 3)) <= 3
    True
    >>> 3 <= len(pronounciable(3, 50)) <= 50
    True
    >>> 8 <= len(pronounciable(8, 10)) <= 10
    True
    >>> 8 <= len(pronounciable(8, 10, tipSpace="all")) <= 10
    True
    """

    # each triplet has 5+2+5+1 bits
    tripbits = 13

    triplet_count = (bits - 1) // tripbits + 1
    minlen = triplet_count * 4 - 3
    maxlen = triplet_count * 4 + 3

    trips = [triplet() for i in range(triplet_count)]
    while len("".join(trips)) > (minlen + maxlen) // 2:
        trips = trips[:-1]

    if tipSpace is None or tipSpace == "numeric":
        tipSpace = numbers
    elif tipSpace == "all":
        tipSpace = numbers + numbers + symbols

    trip_count = len("".join(trips))
    assert trip_count < maxlen
    tipCount = random.randint(max(0, minlen - trip_count), maxlen - trip_count)
    tips = [random.sample(tipSpace, 1)[0] for i in range(tipCount)]

    total = trips + tips
    random.shuffle(total)
    return "".join(total)


def _random(bits, charset):
    middle_bits = bits - 11

    per_word = len(charset).bit_length() - 1
    char_count = (middle_bits - 1) // per_word + 1

    chosen = random.sample(charset, char_count)
    ends = random.sample(alpha + alpha.upper(), 2)
    return ends[0] + "".join(chosen) + ends[1]


def random_pword(bits):
    charset = alpha + alpha.upper() + numbers + symbols
    return _random(bits, charset)


def alphanumeric(bits):
    charset = alpha + alpha.upper() + numbers
    return _random(bits, charset)


def words(bits):
    xwords = word_list.WORD_LIST

    per_word = len(xwords).bit_length() - 1
    word_count = (bits - 1) // per_word + 1

    chosen = random.sample(xwords, word_count)
    return " ".join(chosen)


@app.get("/api/password/generate", name="get_api_password_generate")
def get_api_password_generate(request):
    mode = request.query.get("mode")
    bits = int(request.query.get("bits", 50))

    generator = {
        "pronounciable": pronounciable,
        "words": words,
        "random": random_pword,
        "alphanumeric": alphanumeric,
    }[mode]

    # 128 bit seed
    seed = os.getrandom(16)
    random.seed(seed)

    generated = generator(bits)

    results = api.Results()
    results.keys["password"] = generated
    return results.json_out()
