// Partial v 1.0.2
// https://github.com/StarcounterSamples/Partial
// This is @warpech's experiental Partial (which is Starcounter.Page with implicit standalone mode)
// The goal is to include Partial in Starcounter

using System;
using Starcounter;
using System.Text;

namespace LogStreamer {
    public class Partial : Page {
        public string ImplicitStandaloneTitle = Application.Current.DisplayName;

        public string ImplicitStandaloneTemplate = @"<!DOCTYPE html>

<html>
<head>
    <meta charset=""utf-8"">
    <title>{0}</title>

    <script src=""/sys/object.observe/dist/object-observe.min.js""></script>
    <script src=""/sys/array.observe/array-observe.min.js""></script>

    <script src=""/sys/webcomponentsjs/webcomponents.js""></script>
    <script>
        window.Polymer = window.Polymer || {{}};
        window.Polymer.dom = ""shadow"";
    </script>
    <link rel=""import"" href=""/sys/polymer/polymer.html"">

    <!-- Import Starcounter specific components -->
    <link rel=""import"" href=""/sys/starcounter.html"">
    <link rel=""import"" href=""/sys/starcounter-include/starcounter-include.html"">
    <link rel=""import"" href=""/sys/starcounter-debug-aid/src/starcounter-debug-aid.html"">
    <link rel=""import"" href=""/sys/dom-bind-notifier/dom-bind-notifier.html"">
    <link rel=""import"" href=""/sys/bootstrap.html"">
    <style>
    body {{
      padding: 20px;
      font-size: 14px;
    }}
  </style>
</head>
<body>
    <template is=""dom-bind"" id=""puppet-root""><template is=""imported-template"" content$=""{{{{model.Html}}}}"" model=""{{{{model}}}}""></template>
<dom-bind-notifier path=""model"" observed-object=""{{{{model}}}}"" deep></dom-bind-notifier></template>
    <puppet-client ref=""puppet-root""></puppet-client>
    <starcounter-debug-aid></starcounter-debug-aid>
</body>
</html>";

        private byte[] ImplicitStandalonePageBytes;

        private Boolean IsFullPageHtml(Byte[] html) {
            //TODO test for UTF-8 BOM
            byte[] fullPageTest = Encoding.ASCII.GetBytes("<!"); //full page starts with <!doctype or <!DOCTYPE;
            var indicatorLength = fullPageTest.Length;

            if (html.Length < indicatorLength) {
                return false; // this is too short for a full html
            }

            for (var i = 0; i < indicatorLength; i++) {
                if (html[i] == fullPageTest[i]) {
                    continue;
                }
                return false; //it's a partial
            }

            return true; //it's a full html 
        }

        public override byte[] AsMimeType(MimeType mimeType, out MimeType resultingMimeType, Request request = null) {

            resultingMimeType = mimeType;

            if (mimeType == MimeType.Text_Html) {
                Byte[] byteRepres = Self.GET<Byte[]>(Html);

                // Checking if file not found.
                if (null == byteRepres) {
                    throw new ArgumentOutOfRangeException("Can not find file in Page->Html property: \"" + Html + "\"");
                }

                if (IsFullPageHtml(byteRepres)) {
                    return byteRepres;
                }
                else {
                    if (ImplicitStandalonePageBytes == null) {
                        string html = String.Format(ImplicitStandaloneTemplate, ImplicitStandaloneTitle);
                        ImplicitStandalonePageBytes = Encoding.ASCII.GetBytes(html);
                    }
                    return ImplicitStandalonePageBytes;
                }
            }

            return base.AsMimeType(mimeType, out resultingMimeType, request);
        }
    }
}