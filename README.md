Name: Jayna Bartel (jbarte12)
Module Info: Module 1 Assignment: Personal Website 
     	     Due on 01/25/2025 at 11:59 EST
Approach: 
- Overall: The entire app and its associated information are organized in a
\module_1 project folder. Supporting files in the module_1 folder include a
\PDF of the running website, a requirements.txt file that point to everything
\needed to run the app, and a readme.txt file. To run the program the command
python \run.py can be used and the app runs on 0.0.0.0 at port 8080. It follows
a basic Flask structure covered in class. Within the module_1 folder there is a
script titled run.py and a folder titled app. Within the app folder you will find
the CSS, HTML and Python scripts associated with the app itself. The app folder
contains the scripts __init__.py and pages.py as well as the sub folders static
and templates. The static folder serves as the landing spot for styling pieces
and holds styles.css and the photo used on the homepage titled jayna_headshot.jpg.
The templates folder contains the HTML files that control the layout structure.
Within the templates folder there are 5 HTML files: _navigation.html, base.html,
contact_info.html, home.html and projects.html. The base.html file serves as a
base template. All the scripts are heavily commented, but an overview of what is
happening can be found below.
- The run.py script starts by importing create_app() and defining an instance
of it named app. The __name__ == __main__ statement allows for the script to be
run directly. When executed using python run.py the website will run on host
0.0.0.0 and port 8080.
- The __init__.py script starts by importing Flask, importing pages.py which
defines a blueprint and then goes on to initialize the app and register the blueprint.
- The pages.py script starts by importing Blueprint and render_template and goes
on to create an instance of a blueprint. Routes are then created to the Home,
Contact Info and Projects pages on the website.
- The styles.css script contains the styling rules that are applied to the website.
Things such as font style and size, positioning and colors are controlled here.
This is essentially the file that makes the website look nice.
- The base.html file serves as a baseline for the page templates. It starts by
defining the doc type as html and language as English. It goes on to define the
page information under the head section by defining the character encoding to be
UTF-8 which will allow for special characters if they are eventually added. It
adds a title block and links the styling sheet. It also defines the visible content
information under the body section. Here there is a callout to the navigation
bar since it is on every page. There is also a template for a header and the
main content section.
- The contact_info.html, home.html and projects.html files are all very similar
 and build upon the base.html file. They start by extending/pulling from the
 base.html file. They then define the block header as the title of the page:
 Jayna Bartel and Technical Project Manager, GE Healthcare for Home, Contact
 Info for Contact Info and Projects for Projects. On the Home page there are
 two titles, so h1 and h2 are used to denote different levels of the header and
 h2 appears smaller than h1. The files then go on to define the content of the
 pages. The home page content is my bio and a photo. The photo has code that
 captions the photo Jayna Bartel Headshot if it does not load properly. The bio
 has three headers: Education, Professional Career, and Personal - all of which
 were made bold using strong. Below each header there are paragraphs of information.
 The contact information content is my email which is stylized with strong for
 bold and i for italics. There is also a link to my LinkedIn that includes commands
 to open a new page using target = _blank and not allow external access with
 rel = noopener.  The projects content is very similar but here classes were defined
 in styles.css to control the module title font size and indent the module description.
- The _navigation.html file is a little different. It starts by defining the
navigation section. It then goes on to list the different nav links. Each name
link includes the url based on what was defined in the pages.py script. Each nav
link also has a callout that defines if the page is active. If it is it will be
highlighted in the nav bar. The styling aspect of this is controlled in the styles.css
file.
Known Bugs: There are no known bugs.  


