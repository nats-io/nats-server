// Copyright 2016 Apcera Inc. All rights reserved.

package server

// Holds the root HTML for our monitoring home page.
var rootHTML = "" +
`<html lang="en">
   <head>
     <link href="data:image/x-icon;base64,AAABAAEAEBAAAAEAIABoBAAAFgAAACgAAAAQAAAAIAAAAAEAIAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA+xY1CPsWNwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA+xY1lPsWN/D7FjccAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA+xY2MPsWN/z7Fjf8+xY3HAAAAAAAAAAAAAAAAAAAAAAAAAAA+xY3nPsWN5z7Fjec+xY3nPsWN5z7Fjec+xY3nPsWN/z7Fjf8+xY3/PsWN/z7Fjec+xY3nPsWN5z7Fjec+xY3nPsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/4vcuv/1/Pn/ld/A/z7Fjf8+xY3/PsWN/z7Fjf9KyZP/9fz5//X8+f/1/Pn/ctSr/z7Fjf8+xY3/PsWN/z7Fjf+V38D//////5/ixv8+xY3/PsWN/z7Fjf9Wy5n//////////////////////3nWr/8+xY3/PsWN/z7Fjf8+xY3/ld/A//////+f4sb/PsWN/z7Fjf9o0KP///////////+X3r//yO3d//////951q//PsWN/z7Fjf8+xY3/PsWN/5XfwP//////n+LG/z7Fjf991q////////////9/1rD/PsWN/8jt3f//////edav/z7Fjf8+xY3/PsWN/z7Fjf+V38D//////5/ixv+V3b3///////////9p0aT/PsWN/z7Fjf/I7d3//////3nWr/8+xY3/PsWN/z7Fjf8+xY3/ld/A//////////////////////9Wy5n/PsWN/z7Fjf8+xY3/yO3d//////951q//PsWN/z7Fjf8+xY3/PsWN/5XfwP////////////////9KyZP/PsWN/z7Fjf8+xY3/PsWN/8jt3f//////edav/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3/PsWN/z7Fjf8+xY3//98AAP+fAAD+HwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==" rel="icon" type="image/x-icon"/>
    <style type="text/css">
      body { font-family: “Century Gothic”, CenturyGothic, AppleGothic, sans-serif; font-size: 22; }
      a { margin-left: 32px; }
    </style>
  </head>
  <body>
    <img src="http://nats.io/img/logo.png" alt="NATS">
    <br/>
	<a href=/varz>varz</a><br/>
	<a href=/connz>connz</a><br/>
	<a href=/routez>routez</a><br/>
	<a href=/subsz>subsz</a><br/>
    <br/>
    <a href=http://nats.io/documentation/server/gnatsd-monitoring/>help</a>
  </body>
</html>
`
