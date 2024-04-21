// from DrTM+H

pub struct FastRandom {
    seed: usize,
}

impl FastRandom {
    pub fn new(seed: usize) -> Self {
        let mut random = Self {
            seed: 0,
        };

        random.set_seed0(seed);
        random
    }


    pub fn next(&mut self) -> usize {
        return (self.next_bits(32) << 32) + self.next_bits(32);
    }
    
    fn set_seed0(&mut self, seed: usize) {
        self.seed = (seed ^ 0x5DEECE66Dusize) & ((1usize << 48) - 1);
    }

    fn next_bits(&mut self, bits: usize) -> usize {
        self.seed = (self.seed *  0x5DEECE66Dusize + 0xBusize) & ((1usize << 48) - 1);
        self.seed >> (48 - bits)
    }
}