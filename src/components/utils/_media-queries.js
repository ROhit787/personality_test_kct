import { css } from 'styled-components'

const sizes = {
  giant: 1824,
  laptop: 1224,
  tablet: 775,
  largerPhone: 510
}


export const media = Object.keys(sizes).reduce((accumulator, label) => {
  const emSize = sizes[label] / 16
  accumulator[label] = (...args) => css`
    @media (min-width: ${emSize}em) {
      ${css(...args)};
    }
  `
  return accumulator
}, {})
