import parse from 'html-react-parser';

function Footer() {
  return (
    <div align="right">
      <br/>
      { parse('PLACEHOLDER_FOOTER_CONTENT') }
    </div>
  )
}

export default Footer;
