"""Collection of general purpose building blocks for notebook dashboards."""

import csv
import ipywidgets

gap = ipywidgets.HTML(value='<br>')


def create_toggle(options, callback, orientation="horizontal",
                  button_width="100px", style="info"):
    """Create a toggle with specified callback."""
    children = []
    for o in options:
        b = ipywidgets.Button(
            description=o, button_style=style, width=button_width)
        b.on_click(callback)
        children.append(b)
    if orientation == "horizontal":
        return ipywidgets.HBox(children=children, width="80%")
    else:
        return ipywidgets.VBox(children=children, width="80%")


def create_export(filename, csv_output, download_text="DOWNLOAD_READY"):
    """Create download link for csv export."""
    import urllib
    try:
        data_uri = 'data:application/csv;charset=utf-8,' + urllib.quote(
            csv_output)
        string = "<a content_type='text/csv' href='%s' download='%s'> "
        "<h2>%s</h2> </a>" % (data_uri, filename, download_text)
    except Exception as e:
        string = "Error occurred producing download link: %s" % e
    return string


class UnicodeDictWriter:
    """A CSV dictionary writer which will write rows to CSV file "f",

    which is encoded in the given encoding.
    Modified from UnicodeWriter: https://docs.python.org/2/library/csv.html
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", fieldnames=()):
        # Redirect output to a queue
        import cStringIO
        import codecs
        self.queue = cStringIO.StringIO()
        self.writer = csv.DictWriter(self.queue,
                                     dialect=dialect,
                                     fieldnames=fieldnames)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()
        self.stream.write(','.join(fieldnames) + '\n')

    def writerow(self, rowdict):
        self.writer.writerow({k: str(v).encode("utf-8")
                              for k, v in rowdict.items()})
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

