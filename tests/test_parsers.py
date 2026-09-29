import unittest

from Stage_2_Core_Parser import parse_card
from bs4 import BeautifulSoup


class ExistingStage2ParserTests(unittest.TestCase):
    def test_existing_card_parser_preserves_business_extraction(self):
        html = """
        <div class="Nv2PK">
          <a class="hfpxzc" href="https://maps.example/place/one"
             aria-label="Example Business"></a>
          <a data-value="Website" href="https://example.com"></a>
          <span class="UY7F9">(12)</span>
          <span class="MW4etd">4.5</span>
          <div class="W4Efsd"><span>Restaurant</span></div>
        </div>
        """
        card = BeautifulSoup(html, "html.parser").select_one(".Nv2PK")
        parsed = parse_card(card, {"Search_Keyword": "food", "Search_Location": "Pune"})
        self.assertEqual(parsed["name"], "Example Business")
        self.assertEqual(parsed["website"], "https://example.com")
        self.assertEqual(parsed["reviews"], 12)


if __name__ == "__main__":
    unittest.main()
