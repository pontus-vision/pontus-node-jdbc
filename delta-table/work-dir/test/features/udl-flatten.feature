Feature: Reading JSON Data into DataFrame and flattening it
    
  Scenario: Read JSON data
    Given a JSON object:
      """
      {
        "data" : [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25}
        ],
        "user": {
           "name": "bob"
        }
      }
      """
    When I flatten the object  
    Then the expected flattened columns are "['data__age', 'data__name', 'user__name']"
    And the expected data is:
      | data__age | data__name  | user__name |
      | 30        | Alice       | bob|
      | 25        | Bob         | bob |

  Scenario: Read JSON data nested arrays of jsons
    Given a JSON object:
      """
      {
        "data" : [
            {"name": "Alice", "age": 30, "favourite_colours": [ {"col_name": "blue"},  {"col_name": "green", "col_type": "basic"} ]} ,
            {"name": "Bob", "age": 25}
        ],
        "user": {
           "name": "bob"
        }
      }
      """
    When I flatten the object  
    Then the expected flattened columns are "['data__age', 'data__name', 'user__name', 'data__favourite_colours__col_name', 'data__favourite_colours__col_type']"
    And the expected data is:
      | data__age | data__name| user__name |data__favourite_colours__col_name| data__favourite_colours__col_type |
      | 30        | Alice     | bob        |   blue                          |   None                            |
      | 30        | Alice     | bob        |   green                         |   basic                           |
      | 25        | Bob       | bob        |   None                          |   None                            |

