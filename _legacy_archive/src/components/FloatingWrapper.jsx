import { motion, useSpring } from 'framer-motion';
import { useEffect, useState } from 'react';

const FloatingWrapper = ({ 
  children, 
  delay = 0, 
  duration = 4, 
  yRange = [0, -15], 
  rotateRange = [0, 2], 
  className = "", 
  parallaxStrength = 30,
  isFloating = true // New prop to toggle floating
}) => {
  const [mousePos, setMousePos] = useState({ x: 0, y: 0 });

  useEffect(() => {
    const handleMouseMove = (e) => {
      setMousePos({
        x: (e.clientX / window.innerWidth) - 0.5,
        y: (e.clientY / window.innerHeight) - 0.5,
      });
    };
    window.addEventListener('mousemove', handleMouseMove);
    return () => window.removeEventListener('mousemove', handleMouseMove);
  }, []);

  const springX = useSpring(0, { stiffness: 60, damping: 25 });
  const springY = useSpring(0, { stiffness: 60, damping: 25 });

  useEffect(() => {
    springX.set(mousePos.x * parallaxStrength);
    springY.set(mousePos.y * parallaxStrength);
  }, [mousePos, parallaxStrength, springX, springY]);

  return (
    <motion.div
      className={className}
      style={{ x: springX, y: springY }}
      animate={isFloating ? {
        y: yRange,
        rotate: rotateRange,
      } : {}}
      transition={isFloating ? {
        y: {
          duration: duration,
          repeat: Infinity,
          repeatType: "reverse",
          ease: "easeInOut",
          delay: delay,
        },
        rotate: {
          duration: duration * 1.2,
          repeat: Infinity,
          repeatType: "reverse",
          ease: "easeInOut",
          delay: delay,
        }
      } : {}}
    >
      {children}
    </motion.div>
  );
};

export default FloatingWrapper;
