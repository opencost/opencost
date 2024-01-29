import parse from 'html-react-parser';

function Footer() {
  return (
    <div>
      <br/>
      { parse('PLACEHOLDER_FOOTER_CONTENT') }
    </div>
  )
}

export default Footer;
