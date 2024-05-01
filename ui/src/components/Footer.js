import {Parser as HtmlToReactParser} from 'html-to-react'

// Footer could be HTML, so we need to parse it.
const Footer = () => {
  const content = '<div align="right"><br/>PLACEHOLDER_FOOTER_CONTENT</div>';
  const htmlToReactParser = new HtmlToReactParser();
  const parsedContent = htmlToReactParser.parse(content);
  return (
    parsedContent
    )
  }

export default Footer;
