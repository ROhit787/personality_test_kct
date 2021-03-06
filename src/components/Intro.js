import React from 'react'
import styled from 'styled-components'
import { StartBtn } from '../components/utils/Buttons'
import { IntroCard, CreditsCard } from '../components/utils/Cards'
import { fonts, colors } from '../components/utils/_var'
import { media } from '../components/utils/_media-queries'

const Wrapper = styled.div`
  position: fixed;
  min-height: 100%;
  max-width: 100%;
  background: ${colors.$colorBg};
  h1 {
    position: absoulte;
    font-family: ${fonts.$titleFont};
    font-size: 1.1em;
    color: ${colors.$colorGold};
    text-align: center;
    padding-top: 2em;
    ${media.tablet`font-size: 1.5em; letter-spacing: 1.5px;`};
    ${media.laptop`font-size: 2em; letter-spacing: 2px;`};
  }
  .list-group {
    padding: 0 2em;
    .list-group-item {
      background: transparent;
      padding: 1em 1.25em;
      font-family: ${fonts.$latoFont};
      border: 0;
      margin-bottom: 0;
      color: ${colors.$colorGold};
      ${media.tablet`font-size: 1.3em`};
      text-align: center;
    }
  }
`

const Intro = ({ title, _onStartClick }) => {
  return (
    <Wrapper className="container">
      <IntroCard>
        <div className="corner" />
        <div className="corner" />
        <div className="corner" />
        <div className="corner" />
        <h1>{title}</h1>
        
        <ul className="list-group">
          <li className="list-group-item">Consits of 40 questions</li>
          <li className="list-group-item">Personality begins where comparison ends</li>
          
        </ul>
        <StartBtn onClick={_onStartClick}>
          <span>START</span>
          <div className="icon">
            <i className="fa fa-arrow-right" />
          </div>
        </StartBtn>
      </IntroCard>
      <CreditsCard>
        
        <h1 className="title">KCT BATCH-3</h1>
        <h2 className="dev">17BCS092 <span className="diff">17BCS103</span> 17BCS131 <span className="diff">17BCS073</span></h2>
        </CreditsCard>      
    </Wrapper>
  )
}

export default Intro
