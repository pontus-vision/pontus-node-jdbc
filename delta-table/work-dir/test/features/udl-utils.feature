Feature: Testing UDL utility functions
  @anonymise
  Scenario Outline: Call the UDL anonymise function repeatedly
    Given a call to UDL.anonymise() using "<faker_type>" against "<val>" I expect "<exp>"
  Examples:
    | faker_type | val                       | exp                                        |
    | name       | Leo                       | Jason Brown                                |
    | name       | Leo Martins               | Jacob Stein                                |
    | email      | lmartins@pontusvision.com | brian12@example.net                        |
    | name       | Lua                       | Taylor Williams                            |
    | name       | Leo Martins               | Jacob Stein                                |
    | email      | lmartins@pontusvision.com | brian12@example.net                        |
    | user       | lmartins                  | 'Generator' object has no attribute 'user' |
    | user_name  | lmartins                  | brian12                                    |
    
  Scenario Outline: Get the data type of columns in a data frame
    Given a DAP UDL object with the following data prefix "/opt/spark/work-dir/test/test-001"
    Given a JSON object:
      """
      {
        "foo": "bar"
      }
      """
    When I call the get_dtype() function in dap_udl for column "<col_name>"
    Then the expected col data type is "<col_type>"
   Examples:
     | col_name | col_type     |
     | foo      | string       |
     | fakeCol  | string       |

