html_string = """
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Отчет по платформам</title>
<style>
body {{font-family: Arial;}}

/* Style the tab */
.tab {{
  overflow: hidden;
  border: 1px solid #ccc;
  background-color: #f1f1f1;
}}

/* Style the buttons inside the tab */
.tab button {{
  background-color: inherit;
  float: left;
  border: none;
  outline: none;
  cursor: pointer;
  padding: 14px 16px;
  transition: 0.3s;
  font-size: 17px;
}}

/* Change background color of buttons on hover */
.tab button:hover {{
  background-color: #ddd;
}}

/* Create an active/current tablink class */
.tab button.active {{
  background-color: #ccc;
}}

/* Style the tab content */
.tabcontent {{
  display: none;
  padding: 6px 12px;
  border: 1px solid #ccc;
  border-top: none;
}}

/* includes alternating gray and white with on-hover color */

.striped {{
    font-size: 11pt; 
    font-family: Arial;
    border-collapse: collapse; 
    border: 1px solid silver;

}}

.striped td, th {{
    padding: 5px;
}}

.striped tr:nth-child(even) {{
    background: #E0E0E0;
}}

.striped tr:hover {{
    background: silver;
    cursor: pointer;
}}
</style>
</head>
<body>

<p>
    <a href="{xlsx_location}">Скачать XLSX</a>
</p>

{tabdefinitions}

{tabcontent}

<script>
function openCity(evt, cityName) {{
  var i, tabcontent, tablinks;
  tabcontent = document.getElementsByClassName("tabcontent");
  for (i = 0; i < tabcontent.length; i++) {{
    tabcontent[i].style.display = "none";
  }}
  tablinks = document.getElementsByClassName("tablinks");
  for (i = 0; i < tablinks.length; i++) {{
    tablinks[i].className = tablinks[i].className.replace(" active", "");
  }}
  document.getElementById(cityName).style.display = "block";
  evt.currentTarget.className += " active";
}}

// Get the element with id="defaultOpen" and click on it
document.getElementById("defaultOpen").click();
</script>
   
</body>
</html> 
"""